"""
Real-Postgres multi-process concurrency harness for SKIP LOCKED claim tests.

Lives in an importable module (not ``conftest.py``) so that ``multiprocessing``
spawn workers can re-import the worker function in a fresh interpreter.
``conftest.py`` is collected by pytest and has no stable import name in a
spawned child, which makes worker pickling unreliable across platforms.
"""

from __future__ import annotations

import multiprocessing as mp
import queue as queue_mod
import time
import traceback
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, text


@dataclass(frozen=True)
class ClaimRunResult:
    """Result from a multi-process SKIP LOCKED claim harness run."""

    claimed_ids: list[int]
    by_worker: dict[str, list[int]]


@dataclass
class PostgresConcurrencyHarness:
    """
    Reusable real-Postgres harness for multi-process claim tests.

    Each instance gets a unique table name (uuid-suffixed) so that parallel
    test runs (pytest-xdist, repeated invocations) cannot collide on
    ``DROP TABLE`` / ``CREATE TABLE``.
    """

    database_url: str
    table_name: str = field(default_factory=lambda: f"dewey_skip_locked_{uuid4().hex}")

    def reset_table(self, item_count: int) -> None:
        engine = create_engine(self.database_url)
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{self.table_name}"'))
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE "{self.table_name}" (
                            id integer PRIMARY KEY,
                            claimed_by text,
                            claimed_at timestamptz
                        )
                        """
                    )
                )
                conn.execute(
                    text(f'INSERT INTO "{self.table_name}" (id) SELECT generate_series(1, :count)'),
                    {"count": item_count},
                )
        finally:
            engine.dispose()

    def drop_table(self) -> None:
        engine = create_engine(self.database_url)
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{self.table_name}"'))
        finally:
            engine.dispose()

    def run_claimers(
        self,
        *,
        worker_count: int,
        batch_size: int = 1,
        timeout: float = 30.0,
    ) -> ClaimRunResult:
        """
        Spawn ``worker_count`` processes that race to claim rows via
        ``FOR UPDATE SKIP LOCKED``. Drains the result queue *before* joining
        so a slow worker cannot stall reads from faster ones, and surfaces
        any child-side exception with the original traceback.
        """
        ctx = mp.get_context("spawn")
        result_queue: Any = ctx.Queue()
        workers = [
            ctx.Process(
                name=f"worker-{idx}",
                target=_skip_locked_claim_worker,
                args=(
                    self.database_url,
                    self.table_name,
                    f"worker-{idx}",
                    batch_size,
                    result_queue,
                ),
            )
            for idx in range(worker_count)
        ]

        for worker in workers:
            worker.start()

        by_worker: dict[str, list[int]] = {}
        errors: list[tuple[str, str]] = []
        received = 0
        deadline = time.monotonic() + timeout
        try:
            while received < worker_count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    worker_id, payload = result_queue.get(timeout=remaining)
                except queue_mod.Empty:
                    break
                received += 1
                if isinstance(payload, _WorkerFailure):
                    errors.append((worker_id, payload.traceback))
                else:
                    by_worker[worker_id] = list(payload)
        finally:
            for worker in workers:
                worker.join(timeout=5)
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=2)
                    errors.append((worker.name, f"{worker.name} did not finish within timeout"))
            result_queue.close()
            result_queue.join_thread()

        if errors:
            first_id, first_tb = errors[0]
            extra = f" (+{len(errors) - 1} more)" if len(errors) > 1 else ""
            raise RuntimeError(f"claim worker {first_id!r} failed{extra}:\n{first_tb}")

        if received < worker_count:
            raise RuntimeError(
                f"timed out after {timeout}s waiting for claim results "
                f"(received {received}/{worker_count})"
            )

        claimed_ids = [c for claims in by_worker.values() for c in claims]
        return ClaimRunResult(claimed_ids=claimed_ids, by_worker=by_worker)


@dataclass(frozen=True)
class _WorkerFailure:
    """Sentinel payload carrying a child-side traceback back to the parent."""

    traceback: str


def _skip_locked_claim_worker(
    database_url: str,
    table_name: str,
    worker_id: str,
    batch_size: int,
    result_queue: Any,
) -> None:
    """
    Worker body. Defined at module scope so ``spawn`` can pickle by qualified
    name and re-import in a fresh interpreter. Any unhandled exception is
    converted into a ``_WorkerFailure`` so the parent can re-raise with the
    original traceback instead of seeing an opaque ``queue.Empty``.
    """
    try:
        engine = create_engine(database_url)
        claimed: list[int] = []
        try:
            while True:
                with engine.begin() as conn:
                    rows: Iterable[int] = (
                        conn.execute(
                            text(
                                f"""
                                SELECT id
                                FROM "{table_name}"
                                WHERE claimed_by IS NULL
                                ORDER BY id
                                LIMIT :batch_size
                                FOR UPDATE SKIP LOCKED
                                """
                            ),
                            {"batch_size": batch_size},
                        )
                        .scalars()
                        .all()
                    )
                    row_list = list(rows)
                    if not row_list:
                        break
                    conn.execute(
                        text(
                            f"""
                            UPDATE "{table_name}"
                            SET claimed_by = :worker_id, claimed_at = now()
                            WHERE id = ANY(:ids)
                            """
                        ),
                        {"worker_id": worker_id, "ids": row_list},
                    )
                    claimed.extend(row_list)
        finally:
            engine.dispose()
        result_queue.put((worker_id, claimed))
    except BaseException:  # noqa: BLE001 — forward every child failure
        result_queue.put((worker_id, _WorkerFailure(traceback=traceback.format_exc())))
    finally:
        # Flush the feeder thread before the child exits so the parent never
        # sees a "worker exited 0 but no result" race.
        try:
            result_queue.close()
            result_queue.join_thread()
        except Exception:
            pass
