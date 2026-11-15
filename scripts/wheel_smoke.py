"""End-to-end scenario for the installed wheel.

Runs inside a clean virtualenv (see wheel_smoke.sh), against a fresh database, as a
minimal Django project written to the working directory. Proves the path a first
consumer actually takes:

1. ``migrate`` creates the schema from the shipped migrations
2. a task is declared with ``@dewey.task`` and created inside an atomic block;
   a rolled-back producer transaction leaves nothing behind
3. ``manage.py dewey run`` (one process type, no broker) claims and completes it
4. the worker applies the declared schedule and turns a due tick into a task
5. a failing task dead-letters, and ``dewey dlq list`` shows it
6. ``dewey doctor`` reports ready while the worker runs
7. SIGTERM drains the worker and it exits 0
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

DB_URL = os.environ["DEWEY_SMOKE_DB_URL"]
DB_NAME = os.environ.get("DEWEY_SMOKE_DB_NAME", "dewey_wheel_smoke")

parsed = urlparse(DB_URL)
DB_HOST = parsed.hostname or "localhost"
DB_PORT = parsed.port or 5432
DB_USER = parsed.username or "postgres"
DB_PASSWORD = parsed.password or "postgres"

failures: list[str] = []

SETTINGS = f"""
DATABASES = {{
    "default": {{
        "ENGINE": "django.db.backends.postgresql",
        "NAME": {DB_NAME!r},
        "USER": {DB_USER!r},
        "PASSWORD": {DB_PASSWORD!r},
        "HOST": {DB_HOST!r},
        "PORT": {str(DB_PORT)!r},
    }}
}}
INSTALLED_APPS = ["dewey.django", "smokeapp"]
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
DEWEY = {{
    "AUTO_APPLY_SCHEDULES": True,
    "TIMING": {{
        "renewal_interval_seconds": 1,
        "poll_interval_seconds": 0.2,
        "db_wait_seconds": 1,
        "termination_seconds": 2,
    }},
}}
"""

TASKS = """
import dewey


@dewey.task("smoke.double", schedule=dewey.every(minutes=1))
def double(value: int = 21) -> None:
    assert dewey.current_task().remaining() is not None


@dewey.task("smoke.broken")
def broken() -> None:
    raise ValueError("synthetic failure")
"""


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"    ok  {label}")
    else:
        print(f"    FAIL {label} {detail}")
        failures.append(label)


def recreate_database() -> None:
    if not DB_NAME.endswith("_smoke"):
        raise ValueError("DEWEY_SMOKE_DB_NAME must end with '_smoke'")

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


def write_project() -> dict[str, str]:
    Path("smoke_settings.py").write_text(SETTINGS)
    Path("smokeapp").mkdir(exist_ok=True)
    Path("smokeapp/__init__.py").write_text("")
    Path("smokeapp/tasks.py").write_text(TASKS)
    return dict(os.environ, DJANGO_SETTINGS_MODULE="smoke_settings", PYTHONPATH=os.getcwd())


def wait_for(label: str, condition, seconds: float = 30.0) -> bool:
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if condition():
            check(label, True)
            return True
        time.sleep(0.1)
    check(label, False, "(timed out)")
    return False


def main() -> int:
    print("--> project and schema")
    recreate_database()
    env = write_project()
    os.environ.update(env)
    sys.path.insert(0, os.getcwd())
    manage = [sys.executable, "-m", "django"]
    subprocess.run([*manage, "migrate", "--no-input", "-v", "0"], check=True, env=env)

    import django

    django.setup()
    from django.db import connection, transaction

    from dewey.django import create_task
    from dewey.django.models import ScheduleEntry, TaskEntry

    def status(task_id: str) -> str:
        return TaskEntry.objects.get(id=task_id).status

    print("--> producing work")
    with transaction.atomic():
        task = create_task(task_type="smoke.double", kwargs={"value": 21})
    # The attempt budget is stamped at creation, and a producer does not import handlers.
    doomed = create_task(task_type="smoke.broken", max_attempts=1)
    try:
        with transaction.atomic():
            create_task(task_type="smoke.double", idempotency_key="rolled-back")
            raise RuntimeError("producer changed its mind")
    except RuntimeError:
        pass
    check(
        "a rolled-back producer leaves no task",
        not TaskEntry.objects.filter(idempotency_key="rolled-back").exists(),
    )

    print("--> one worker process, no broker")
    worker = subprocess.Popen([*manage, "dewey", "run"], env=env)
    try:
        wait_for("the task completed", lambda: status(task.id) == "completed")
        wait_for("the failing task dead-lettered", lambda: status(doomed.id) == "dead")
        wait_for(
            "the worker applied the declared schedule",
            lambda: ScheduleEntry.objects.filter(name="smoke.double").exists(),
        )
        with connection.cursor() as cursor:  # make the next tick due now
            cursor.execute(
                "UPDATE dewey_schedules SET cursor_at = clock_timestamp() - interval '3 min',"
                " next_due_at = clock_timestamp() - interval '3 min'"
            )
        ticks = TaskEntry.objects.filter(idempotency_key__startswith="schedule:smoke.double:")
        # A minute boundary can add a tick of its own next to the rewound one.
        finished = {"completed", "expired"}
        wait_for(
            "a due tick became a completed task",
            lambda: (
                "completed" in (seen := {tick.status for tick in ticks.all()}) and seen <= finished
            ),
        )

        print("--> the command line")
        dewey = [str(Path(sys.executable).with_name("dewey")), "--json"]
        app = ["--app", "dewey.django.worker:create_application"]
        listed = subprocess.run(
            [*dewey, *app, "dlq", "list"], env=env, capture_output=True, text=True
        )
        letters = json.loads(listed.stdout) if listed.returncode == 0 else []
        check("dlq list shows the dead letter", [row["id"] for row in letters] == [doomed.id])
        doctor = subprocess.run(
            [*manage, "dewey", "--json", "doctor"], env=env, capture_output=True, text=True
        )
        check("doctor reports ready", doctor.returncode == 0, doctor.stdout[-500:])

        print("--> shutdown")
        worker.send_signal(signal.SIGTERM)
        check("SIGTERM drains the worker and it exits 0", worker.wait(timeout=60) == 0)
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait()

    if failures:
        print(f"\n{len(failures)} check(s) failed: {', '.join(failures)}")
        return 1
    print("\nAll installed-wheel checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
