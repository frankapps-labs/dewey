"""End-to-end scenario for the installed wheel.

Runs inside a clean virtualenv (see wheel_smoke.sh), against a fresh database, as a
minimal Django project. Proves the path a first consumer actually takes:

1. ``migrate`` creates the schema from the shipped migrations
2. a task is declared with ``@dewey.task`` and created inside an atomic block
3. the dispatcher claims it and hands the ID to Huey
4. a Huey worker processes it through Dewey's state machine to COMPLETED

Then the failure modes that matter operationally:

5. a task committed while Redis is unreachable survives and dispatches after recovery
6. a duplicate delivery is harmless
7. a rolled-back producer transaction leaves nothing to dispatch
8. a failing handler retries on Dewey's schedule and dead-letters on budget
"""

from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

DB_URL = os.environ["DEWEY_SMOKE_DB_URL"]
REDIS_URL = os.environ["DEWEY_SMOKE_REDIS_URL"]
DB_NAME = os.environ.get("DEWEY_SMOKE_DB_NAME", "dewey_wheel_smoke")

parsed = urlparse(DB_URL)
DB_HOST = parsed.hostname or "localhost"
DB_PORT = parsed.port or 5432
DB_USER = parsed.username or "postgres"
DB_PASSWORD = parsed.password or "postgres"

failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"    ok  {label}")
    else:
        print(f"    FAIL {label} {detail}")
        failures.append(label)


def recreate_database() -> None:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{DB_NAME}"')
        cur.execute(f'CREATE DATABASE "{DB_NAME}"')
    conn.close()
    print(f"    ok  fresh database {DB_NAME}")


def configure_django() -> None:
    import django
    from django.conf import settings

    from huey import MemoryHuey

    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": DB_NAME,
                "USER": DB_USER,
                "PASSWORD": DB_PASSWORD,
                "HOST": DB_HOST,
                "PORT": str(DB_PORT),
            }
        },
        INSTALLED_APPS=["dewey.django"],
        USE_TZ=True,
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        HUEY=MemoryHuey("dewey-contrib-smoke", immediate=True),
        DEWEY={"DISPATCH": "dewey.contrib.django_huey.dispatch"},
    )
    django.setup()


# --- transport wiring, exactly as a consumer would do it -------------------------

huey = None
adapter = None
processed: list[tuple[int, ...]] = []


def dispatch(task_id: str):  # named in DEWEY["DISPATCH"]
    assert adapter is not None
    return adapter.dispatch(task_id)


def build_transport(*, immediate: bool) -> None:
    """Wire Huey and register the processor, the way a shared tasks module would."""
    global huey, adapter
    from huey import RedisHuey

    from dewey.adapters.huey import HueyAdapter
    from dewey.django.executor import process_task

    huey = RedisHuey("dewey-smoke", url=REDIS_URL, immediate=immediate, results=False)
    adapter = HueyAdapter(huey)
    adapter.register(process_task)


def main() -> int:
    global adapter

    print("--> preparing a fresh database")
    recreate_database()
    configure_django()

    import json
    from io import StringIO

    from django.core.management import call_command
    from django.db import transaction

    print("--> migrate from the shipped migrations")
    call_command("migrate", verbosity=0)
    from dewey.django.models import TaskEntry

    check("migrate created task_entries", TaskEntry.objects.count() == 0)

    import dewey
    from dewey.dispatcher import Dispatcher
    from dewey.django.dispatch import DjangoDispatchBackend
    from dewey.contrib.django_huey import dispatch as contrib_dispatch
    from dewey.django.executor import create_or_get_task, create_task
    from dewey.errors import IdempotencyConflictError

    @dewey.task("smoke.notify", max_attempts=3, backoff=dewey.Constant(1))
    def notify(command_id: int) -> None:
        processed.append((command_id,))

    @dewey.task("smoke.always_fails", max_attempts=2, backoff=dewey.Constant(0))
    def always_fails(command_id: int) -> None:
        raise dewey.TransientError("recipient unavailable")

    backend = DjangoDispatchBackend()
    dispatcher = Dispatcher(backend, contrib_dispatch, sweep_interval_seconds=None)
    check(
        "preferred contrib dispatch is module-level",
        callable(__import__("dewey.django.conf", fromlist=["get_dispatch_fn"]).get_dispatch_fn()),
    )

    print("--> happy path: create, dispatch, process, complete")
    with transaction.atomic():
        task = create_task(task_type="smoke.notify", args=[42])
    dispatched = dispatcher.dispatch_batch()
    row = TaskEntry.objects.get(id=task.id)
    check("one task dispatched", dispatched == 1, f"got {dispatched}")
    check("handler ran with its argument", processed == [(42,)], f"got {processed}")
    check("row completed", row.status == "completed", f"status={row.status}")
    check("one attempt recorded", row.attempts == 1, f"attempts={row.attempts}")
    check("completion timestamped", row.completed_at is not None)

    print("--> duplicate delivery is harmless")
    processed.clear()
    contrib_dispatch(task.id)
    check("handler did not run again", processed == [], f"got {processed}")

    print("--> rolled-back producer leaves nothing to dispatch")
    before = TaskEntry.objects.count()
    try:
        with transaction.atomic():
            create_task(task_type="smoke.notify", args=[99])
            raise RuntimeError("business logic failed")
    except RuntimeError:
        pass
    check("no row written", TaskEntry.objects.count() == before)
    check("nothing to claim", backend.claim(10) == [])

    print("--> broker outage: work waits in Postgres, then dispatches on recovery")

    def broken_dispatch(task_id: str) -> None:
        raise ConnectionError("redis is unreachable")

    outage_dispatcher = Dispatcher(backend, broken_dispatch, sweep_interval_seconds=None)
    with transaction.atomic():
        outage_task = create_task(task_type="smoke.notify", args=[7])
    outage_dispatcher.dispatch_batch()
    row = TaskEntry.objects.get(id=outage_task.id)
    check("survives the outage as pending", row.status == "pending", f"status={row.status}")
    check("outage did not burn an attempt", row.attempts == 0, f"attempts={row.attempts}")

    processed.clear()
    recovered = Dispatcher(backend, contrib_dispatch, sweep_interval_seconds=None)
    check("dispatches after recovery", recovered.dispatch_batch() == 1)
    check("handler ran once recovered", processed == [(7,)], f"got {processed}")

    print("--> failure path: retry on Dewey's schedule, then dead-letter")
    with transaction.atomic():
        failing = create_task(task_type="smoke.always_fails", args=[1])
    recovered.dispatch_batch()
    row = TaskEntry.objects.get(id=failing.id)
    check("first failure retries", row.status == "failed", f"status={row.status}")
    check("retry is scheduled by Dewey", row.scheduled_for is not None)

    TaskEntry.objects.filter(id=failing.id).update(
        scheduled_for=datetime.now(UTC) - timedelta(seconds=1)
    )
    # A due retry is directly claimable; the 60-second recovery sweep is not in
    # the latency path.
    recovered.dispatch_batch()
    row = TaskEntry.objects.get(id=failing.id)
    check("dead-letters once the budget is spent", row.status == "dead", f"status={row.status}")
    check("dead task is queryable with its error", "unavailable" in row.error)

    print("--> expiry and idempotent creation")
    expired = create_task(
        task_type="smoke.notify",
        args=[404],
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    processed.clear()
    check("expired task is not dispatched", recovered.dispatch_batch() == 0)
    expired_row = TaskEntry.objects.get(id=expired.id)
    check("expired task is auditable", expired_row.status == "expired")
    check("expiry consumes no attempt", expired_row.attempts == 0)
    first = create_or_get_task(
        task_type="smoke.notify",
        idempotency_key="smoke-idempotent-001",
        args=[55],
    )
    same = create_or_get_task(
        task_type="smoke.notify",
        idempotency_key="smoke-idempotent-001",
        args=[55],
    )
    check("identical idempotent creation returns one task", first.id == same.id)
    try:
        create_or_get_task(
            task_type="smoke.notify",
            idempotency_key="smoke-idempotent-001",
            args=[56],
        )
    except IdempotencyConflictError as exc:
        check("conflicting key is typed and redacted", exc.differing_fields == ("args",))
    else:
        check("conflicting key is typed and redacted", False)
    recovered.dispatch_batch()  # drain the one idempotent task before the Redis leg

    print("--> doctor sees a fresh dispatcher heartbeat")
    backend.heartbeat()
    doctor_output = StringIO()
    call_command("dewey_doctor", "--format", "json", stdout=doctor_output)
    doctor = json.loads(doctor_output.getvalue())
    check("doctor JSON reports ready", doctor["ok"] is True)

    print("--> real Redis round trip through a Huey consumer")
    processed.clear()
    build_transport(immediate=False)
    assert huey is not None
    try:
        huey.storage.flush_queue()
    except Exception as exc:  # pragma: no cover — surfaced, not swallowed
        print(f"    FAIL could not reach Redis at {REDIS_URL}: {exc}")
        failures.append("redis reachable")
    else:
        real_dispatcher = Dispatcher(backend, dispatch, sweep_interval_seconds=None)
        with transaction.atomic():
            queued = create_task(task_type="smoke.notify", args=[123])
        real_dispatcher.dispatch_batch()
        check(
            "task ID is on the Redis queue",
            huey.storage.queue_size() == 1,
            f"size={huey.storage.queue_size()}",
        )
        check(
            "row is dispatching until a worker takes it",
            TaskEntry.objects.get(id=queued.id).status == "dispatching",
        )

        # Drain the queue the way huey_consumer would.
        deadline = time.monotonic() + 10
        while huey.storage.queue_size() and time.monotonic() < deadline:
            message = huey.dequeue()
            if message is None:
                break
            # Depending on the Huey version, dequeue() hands back a Task or the raw
            # serialised message.
            task = message if hasattr(message, "id") else huey.deserialize_task(message)
            huey.execute(task)
        row = TaskEntry.objects.get(id=queued.id)
        check("worker completed it", row.status == "completed", f"status={row.status}")
        check("handler ran via Redis", processed == [(123,)], f"got {processed}")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {', '.join(failures)}")
        return 1
    print("All installed-wheel checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
