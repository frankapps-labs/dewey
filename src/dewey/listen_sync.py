"""Blocking Postgres LISTEN for synchronous dispatchers.

Wake-up is an optimisation, never a guarantee. The dispatcher's poll is what
makes delivery correct — it finds work whose notification was missed, and work
that only became due because ``scheduled_for`` passed. This module just shortens
the wait when a notification does arrive.

Works with psycopg2 (``select`` on the connection, then ``poll``) and psycopg3
(``conn.notifies``). If neither is available, :class:`SyncWorkListener` reports
``supported = False`` and the dispatcher falls back to sleeping, which costs
latency and nothing else.
"""

from __future__ import annotations

import json
import logging
import select
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_WORK_CHANNEL = "dewey_work_available"


def work_payload(kind: str, entry_id: str, queue: str | None = None) -> str:
    """The NOTIFY payload Dewey sends. One shape for every backend.

    Listeners only need the wake-up, not the contents — the database is still the
    source of truth — but keeping one format means a dispatcher can be woken by a
    producer running on either ORM.
    """
    return json.dumps({"kind": kind, "id": entry_id, "queue": queue}, separators=(",", ":"))


def _quote_identifier(name: str) -> str:
    """Quote a channel name for use in LISTEN, which cannot take a parameter."""
    return '"' + name.replace('"', '""') + '"'


class SyncWorkListener:
    """Own one dedicated connection and block until Postgres says there is work.

    The connection is separate from the one the dispatcher claims rows with,
    because a connection sitting in LISTEN cannot be used for anything else.

    Args:
        connect: Returns a *new* raw DBAPI connection. Called once, on enter.
        channel: Postgres channel to listen on.
    """

    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        channel: str = DEFAULT_WORK_CHANNEL,
    ) -> None:
        self._connect = connect
        self.channel = channel
        self._conn: Any | None = None
        self._flavour: str | None = None

    # --- lifecycle ---

    def __enter__(self) -> SyncWorkListener:
        self.open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def open(self) -> None:
        if self._conn is not None:
            return
        conn = self._connect()
        try:
            self._flavour = _detect_flavour(conn)
            if self._flavour is None:
                _safe_close(conn)
                logger.info(
                    "Dewey dispatcher: LISTEN is unavailable for this driver; "
                    "falling back to polling only."
                )
                return
            _set_autocommit(conn, self._flavour)
            cursor = conn.cursor()
            try:
                cursor.execute(f"LISTEN {_quote_identifier(self.channel)}")
            finally:
                cursor.close()
            self._conn = conn
            logger.info("Dewey dispatcher listening on Postgres channel %s", self.channel)
        except Exception:
            _safe_close(conn)
            raise

    def close(self) -> None:
        if self._conn is not None:
            _safe_close(self._conn)
            self._conn = None

    @property
    def supported(self) -> bool:
        """True when a notification can actually wake this listener."""
        return self._conn is not None

    # --- waiting ---

    def wait(self, timeout: float) -> bool:
        """Block up to ``timeout`` seconds. True if a notification arrived.

        Spurious wake-ups are fine and expected: the caller re-checks the
        database either way.
        """
        conn = self._conn
        if conn is None:
            return False
        try:
            if self._flavour == "psycopg3":
                return self._wait_psycopg3(conn, timeout)
            return self._wait_psycopg2(conn, timeout)
        except Exception:
            # A dropped listen connection must not take the dispatcher down: the
            # poll loop is still correct without it.
            logger.warning(
                "Dewey dispatcher: LISTEN connection failed; continuing with polling.",
                exc_info=True,
            )
            self.close()
            return False

    def _wait_psycopg2(self, conn: Any, timeout: float) -> bool:
        if not select.select([conn], [], [], timeout)[0]:
            return False
        conn.poll()
        received = bool(conn.notifies)
        del conn.notifies[:]
        return received

    def _wait_psycopg3(self, conn: Any, timeout: float) -> bool:
        # psycopg 3.2 added a timeout to notifies(); on 3.0/3.1 wait on the socket
        # ourselves and then drain without blocking.
        try:
            generator = conn.notifies(timeout=timeout, stop_after=1)
        except TypeError:
            if not select.select([conn.fileno()], [], [], timeout)[0]:
                return False
            conn.execute("SELECT 1")  # consume input so notifies are materialised
            return True
        return any(True for _ in generator)


def _detect_flavour(conn: Any) -> str | None:
    module = type(conn).__module__ or ""
    if module.startswith("psycopg2"):
        return "psycopg2"
    if module.startswith("psycopg"):
        return "psycopg3"
    # Some wrappers proxy the driver connection; fall back to capability sniffing.
    if hasattr(conn, "notifies") and callable(conn.notifies):
        return "psycopg3"
    if hasattr(conn, "poll") and hasattr(conn, "notifies"):
        return "psycopg2"
    return None


def _set_autocommit(conn: Any, flavour: str) -> None:
    """LISTEN only delivers outside a transaction."""
    if flavour == "psycopg2":
        conn.set_session(autocommit=True)
    else:
        conn.autocommit = True


def _safe_close(conn: Any) -> None:
    try:
        conn.close()
    except Exception:  # pragma: no cover — closing a broken connection
        logger.debug("Dewey dispatcher: error closing LISTEN connection", exc_info=True)


__all__ = ["DEFAULT_WORK_CHANNEL", "SyncWorkListener", "work_payload"]
