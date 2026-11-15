# The worker

`dewey run` is the only process type Dewey needs. It claims committed task rows from
Postgres, runs each in its own process, renews and recovers leases, creates due
[schedule](schedules.md) ticks and writes a heartbeat. There is no broker and no
separate dispatcher or scheduler: run as many copies as you need.

```bash
dewey run --app myapp.worker:create_application --queues default,reports --concurrency 4
python manage.py dewey run          # Django: settings-driven, no factory to write
```

It is a POSIX service with process isolation: one task per slot, sync or async
handlers, Django or SQLAlchemy (sync or async) storage.

**Deployment requirement:** run it inside a container, cgroup or equivalent kill-all
boundary. If the worker's own supervisor is killed outright, only that enclosing
boundary can guarantee no task process survives it. Containers on any orchestrator
qualify. This is not a process sandbox or an exactly-once engine: keep handlers
[idempotent](idempotency.md).

## Application factory

Provide an importable **synchronous** `module:function` factory. It is called
separately inside the worker parent and in each task process, so it must create its
own engine or connections every time: never share them across processes. Import your
handler modules in it; a worker claims only the task types it has handlers for.

```python
# myapp/worker.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dewey.worker import WorkerContext


def create_application(context: WorkerContext):
    from myapp import tasks  # registers @dewey.task handlers and schedules

    engine = create_engine(os.environ["DATABASE_URL"], pool_size=1, max_overflow=0)
    return context.sqlalchemy(sessionmaker(engine), close_resources=engine.dispose)
```

For async SQLAlchemy use `context.async_sqlalchemy(async_sessionmaker(engine),
close_resources=engine.dispose)`. Django projects need no factory:
`manage.py dewey run` sets Django up in each process, imports every installed app's
`tasks` module and uses the alias in `DEWEY["DATABASE"]`. To write one anyway, call
`django.setup()`, import your handlers and `return context.django(using="default")`.

`context.owner` is a fresh instance ID for this service; `context.role` is `parent`
or `task`. The worker never creates or migrates tables: apply migrations first.

Bound your driver's connect and network waits (`connect_timeout`, TCP keepalives). A
SQL timeout cannot rescue a connection that never answers.

## Settings

Only the factory is required. Everything else has a default, and every default can be
set in a JSON file passed with `--config` (flags override the file):

```json
{
  "factory": "myapp.worker:create_application",
  "queues": ["default"],
  "concurrency": 2,
  "max_restarts": 3,
  "startup_seconds": 30,
  "supervision_seconds": 30,
  "shutdown_seconds": 30,
  "auto_apply_schedules": false,
  "alert_hook": null,
  "timing": {
    "task_timeout_seconds": 3600,
    "renewal_interval_seconds": 5,
    "poll_interval_seconds": 1,
    "db_wait_seconds": 2,
    "termination_seconds": 4
  }
}
```

| Setting | Meaning |
|---|---|
| `queues` | Queues this worker serves. Run separate workers for queues that must not share capacity. |
| `concurrency` | Task processes. Each runs one task at a time. |
| `shutdown_seconds` | How long a stopping worker waits for running tasks. Keep it below your orchestrator's stop timeout. |
| `max_restarts` | Task-process crashes tolerated in one service lifetime before the worker exits with a fault. |
| `auto_apply_schedules` | Apply declared schedules at start. Single-node installs only. |
| `alert_hook` | `module:function` for [alerts](operations.md#alerts). |
| `timing.task_timeout_seconds` | Hard ceiling for any task on this worker. A task may set a shorter `execution_timeout_s`. |
| `timing.poll_interval_seconds` | Pause between rounds when idle: the longest a new task waits for an idle worker. |
| `timing.renewal_interval_seconds` | How often leases are renewed, recovery runs, the heartbeat is written and due ticks are created. |

Leases (`assignment_lease_seconds`, `execution_lease_seconds`) are derived from the
other limits and the concurrency so that a worst-case round always fits inside them;
set them only if you have measured a reason to. A task whose worker dies is handed
back about one lease later (a minute, by default) and then waits out its retry
backoff like any failed attempt: with the default policy it runs again roughly three
minutes after the worker died. Unknown settings are rejected. Keep
credentials out of this file: the factory owns connection settings.

In Django the same settings live in `settings.DEWEY`:

```python
DEWEY = {
    "DATABASE": "default",
    "QUEUES": ("default",),
    "CONCURRENCY": 2,
    "AUTODISCOVER": "tasks",
    "AUTO_APPLY_SCHEDULES": False,
    "ALERT_HOOK": None,
    "TIMING": {},          # e.g. {"task_timeout_seconds": 600}
}
```

From Python, `run_worker(WorkerConfig(factory="myapp.worker:create_application"))`
does the same and returns the exit status. It is a blocking main-thread entrypoint,
not something to call from a request handler or an event loop.

## Time limits a handler can rely on

- **A hard timeout.** A task that runs past `task_timeout_seconds`, or its own
  `execution_timeout_s` set at creation, has its process stopped. The attempt counts,
  and the task is retried per its policy.
- **A deadline it can read.** `dewey.current_task().deadline` is that moment and
  `.remaining()` the time left, so a long handler can stop cleanly between units of
  work instead of being cut off mid-write:

  ```python
  for batch in batches:
      if dewey.current_task().remaining() < timedelta(seconds=30):
          raise dewey.RetryAfter(0)      # pick up the rest on the next attempt
      process(batch)
  ```

- **Database budgets per task.** `@dewey.task("reports.build", statement_timeout_s=30,
  lock_timeout_s=5)` bounds the handler's own queries. The Django worker applies them
  to its connection for the attempt and restores it afterwards. With SQLAlchemy the
  handler owns its connections, so apply them where you open one:

  ```python
  from dewey.sqlalchemy import apply_task_timeouts

  with Session(engine) as session, session.begin():
      apply_task_timeouts(session)       # SET LOCAL from the current task's policy
      ...
  ```

## Supervision pacing

`poll_interval_seconds` bounds the pause between supervision rounds. When a task
child is active or initializing, the parent can wake earlier on its existing
control pipe or process-exit sentinel. A readiness signal is only a hint: the next
round still polls, reconciles durable ownership/outcome and acknowledges the slot
before reusing capacity. Waiting does not read messages or change leases, database
timeouts, task attempts or shutdown containment.

When all children are idle and ready, the parent retains the configured polling
cadence; this does not add a database listener or busy idle polling. Unchanged pipe
readability (including EOF while a child remains alive) suppresses only that
unchanged pipe to avoid spinning; other child pipes and process-exit sentinels
remain watched within the requested interval. Slot, connection or assignment
progress restores pipe watching. During drain, waits also respect the remaining
shutdown budget. These bounds do not make unbounded application/driver calls safe.

## Containment, drain and exit status

- The outer supervisor performs no DB/handler work. Startup is independently
  bounded; a stalled parent's progress protocol also has an independent deadline.
  A live process alone is not proof of healthy ledger supervision.
- SIGTERM/SIGINT to the supervisor requests parent drain, stops new claims (except
  already-in-flight claims), and lets active assignments reconcile within the
  explicit shutdown budget. Budget expiry forces owned-group termination. Handler
  effects may already exist; unsettled obligations recover after real lease expiry.
- Unexpected parent exit, including unexpected exit0, is a fault. Terminate its
  exclusive group, escalate TERM to KILL and verify reaping before returning. Linux
  subreaper handling also reaps adopted orphan workers/trackers (including PID1).
- Exit **0** means requested graceful stop; **70** means fault/forced stop after
  containment; **71** means uncertain cleanup. **Do not automatically restart on
  71** until the enclosing runtime proves all old descendants are gone. Invalid
  configuration is rejected by the CLI before launch.
- A database that is reachable but too slow for `db_wait_seconds` does not stop the
  worker at once. An operation that ran out of time rolled back, so the worker holds
  its work in progress, logs a warning and keeps trying; task processes do the same
  with their own start and finish calls. It stops with **70** only when its leases
  can no longer be trusted (about `execution_lease_seconds` minus the time needed to
  stop every task process, since they were last renewed). A lost connection, or any
  failure whose outcome is unknown, still stops it immediately.
- The command does not automatically restart, alter schema, reset tasks, dump
  payloads or print raw DB exception strings. Configure bounded/backoff restart
  policy in the service manager, with fresh identity on replacement.

Supervision does not atomically cancel external effects, guarantee termination of
kernel-uninterruptible processes, or fix unbounded custom driver/resource hooks.
Cleanup uncertainty stays unhealthy, not a success report. Readiness and SQL
budgets are not a substitute for real network/COMMIT, root-death, shutdown and
idempotent-effect qualification in the actual deployment environment.
