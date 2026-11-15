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
  "start_method": "spawn",
  "tracing": false,
  "failure_details": "type",
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
| `startup_seconds` | How long the application import, and then each task process, may take without reporting progress. Raise it when the application is slow to import on a small CPU. |
| `supervision_seconds` | How long the running service may go without reporting progress before the supervisor stops it. |
| `max_restarts` | Task-process crashes tolerated in one service lifetime before the worker exits with a fault. |
| `start_method` | `spawn` (default) gives every process its own import of the application; `fork` imports it once and forks, sharing memory. See [process model and memory](#process-model-and-memory) and [choosing a start method](#choosing-a-start-method). |
| `auto_apply_schedules` | Apply declared schedules at start. Single-node installs only. |
| `alert_hook` | `module:function` for [alerts](operations.md#alerts). |
| `tracing` | Open an OpenTelemetry span per attempt in the task processes. Needs `dewey[otel]`; see [tracing](logging.md#opentelemetry-spans). |
| `failure_details` | What a failed attempt's log record says about the exception: `type` (its class only, the default) or `full` (message and traceback too). See [failed attempts](logging.md#failed-attempts). |
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
    "START_METHOD": "spawn",
    "STARTUP_SECONDS": 30.0,
    "SUPERVISION_SECONDS": 30.0,
    "SHUTDOWN_SECONDS": 30.0,
    "MAX_RESTARTS": 3,
    "TRACING": False,      # an OpenTelemetry span per attempt; needs dewey[otel]
    "FAILURE_DETAILS": "type",  # or "full": exception message and traceback in the log
}
```

From Python, `run_worker(WorkerConfig(factory="myapp.worker:create_application"))`
does the same and returns the exit status. It is a blocking main-thread entrypoint,
not something to call from a request handler or an event loop.

## Time limits a handler can rely on

- **A hard timeout.** A task that runs past `task_timeout_seconds`, or its own
  `execution_timeout_s`, has its process stopped. The attempt counts, and the task
  is retried per its policy. Set the limit per task type in the policy
  (`@dewey.task("photos.moderate", execution_timeout_s=90)` or
  `configure_policies`), or per task with `create_task(execution_timeout_s=...)`;
  it is stamped onto the row at creation, like `max_attempts`. A task type that
  decodes a photo and calls a moderation API should die after a minute or two
  instead of holding a slot for the worker's default hour. The policy cannot know
  which worker will run the task, so `task_timeout_seconds` stays the ceiling: a
  larger `execution_timeout_s` is bounded by it when the task is claimed.
- **A deadline it can read.** `dewey.current_task().deadline` is that moment and
  `.remaining()` the time left, so a long handler can stop cleanly between units of
  work instead of being cut off mid-write (`dewey.current_task_or_none()` returns
  `None` when the handler is called outside a task):

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

## Process model and memory

`dewey run` is three kinds of process: the supervisor (the process you started, which
never touches the database or a handler), the service (claims, renews, recovers and
creates ticks) and one task process per slot. With the default `start_method` of
`spawn`, the service and every task process is a fresh interpreter that imports the
application through the factory. That is the safest model: nothing from one process
can leak into another. It also means each process holds its own copy of the imported
application (a Django project with a few libraries is 50-90 MB), and that each import
costs CPU at startup.

Task processes start one after another: the next starts once the newest has reported
ready, and the worker claims as soon as the first is ready. `startup_seconds` bounds
each of them separately, measured from its own start, and bounds the application import
in the service. A worker with eight slots on a quarter of a CPU therefore takes a
while to reach full capacity but runs its first task early and does not fail just
because the slots together take longer than `startup_seconds`. If a single import
does take longer than that, the worker stops with a message naming
`startup_seconds`; raise it, or give the worker more CPU.

`start_method: "fork"` imports the application once and forks the task processes
from the process that holds it. They share the imported application copy-on-write,
so a slot costs the memory it touches (tens of megabytes) instead of a full copy,
and a task process starts in milliseconds. This is what Celery's prefork pool does,
with the same requirement: the application must be fork-safe. Which process imports
it depends on how the worker was started:

- **The plain CLI** (`dewey run --app ...`, `python -m dewey.worker`) imports
  nothing of the application in the launching process. The supervisor forks the
  service, the service imports the application through the factory, and the task
  processes are forked from the service. The service is the process that must be
  fork-safe: what its factory opened (connections, threads) is what a task process
  would inherit, and `close_resources` runs before every fork for exactly that.
- **A host that has already imported the application**, such as `manage.py dewey
  run` (the management command runs after Django's setup), forks the service from
  that process, so the service and every task process share the host's copy. Here
  the launching process must be fork-safe too: nothing it opened before `dewey run`
  (a connection made at import, a thread started by a library) may be live at the
  fork. The memory figures in this document and the changelog were measured on
  `manage.py dewey run` (a production Django 5.2 project); with the plain CLI the saving
  is the same across task processes, the launching process just holds no copy.

- Before every fork the worker calls the application's `close_resources` so no
  database connection crosses into the child. Django's `connections.close_all()`
  and SQLAlchemy's `engine.dispose()` both leave the parent able to reconnect on its
  next use, which is what a factory's `close_resources` must do under `fork`.
- The process a child is forked from (the service; with a preloaded host also the
  launching process) must hold no database connection and no thread that a forked
  child could deadlock on. `manage.py dewey run` qualifies: the system checks run
  without connecting. A library that starts a background thread on import (a
  telemetry exporter, for example) is the usual exception; check its own fork
  guidance before switching. The OpenTelemetry SDK's batch span processor restarts
  its export thread in a forked child, and Dewey's own test suite exports spans
  from forked task processes; see [tracing](logging.md#opentelemetry-spans).
- A forked service exits through `os._exit`, so `atexit` hooks do not run in it.
  Dewey shuts logging down first, so a buffered or queue-based log handler still
  delivers the lines that explain an exit 70 or 71; a library that flushes
  telemetry from an `atexit` hook (rather than from an `os._exit` patch, as
  Logfire does) flushes in a spawned service but not in a forked one.
- Task processes reset the signal handlers they inherit, so SIGTERM still stops
  them. Handlers see their own fresh connections and event loop.
- Use it on Linux. macOS forbids some system frameworks after a fork, and the
  default `spawn` stays the right choice there and on every platform where you have
  not verified the application.

## Choosing a start method

`spawn` stays the default because it is safe for any application; `fork` is the
recommendation for Linux containers once the checklist below passes.

| | `spawn` | `fork` |
|---|---|---|
| Isolation | Every process is a fresh interpreter: nothing from one leaks into another. | Task processes are copies of the service (with `manage.py dewey run`, of the host that imported the application); anything that process opened or started before the fork is shared with them. |
| Memory per slot | A full copy of the imported application (50-90 MB for a Django project; `manage.py dewey run` in a production Django project measured 85 MB per slot plus about 195 MB fixed: 365 MB at concurrency 2 on 0.6.0). | The pages a slot touches, tens of MB; `manage.py dewey run` in the same project at concurrency 8 is about 215 MB in all. |
| Time to first task | About 9 s on 0.25 vCPU (one application import, then the first slot's). | About 4 s on 0.25 vCPU (one import, then forks in milliseconds). |
| Time to full capacity | One import per slot, one after another: eight slots on 0.25 vCPU take a minute or so. | Seconds. |
| Restart after a crash or recycle | A fresh import, paid again each time. | A fork, in milliseconds. |
| Platforms | Linux, macOS. | Linux. macOS forbids some system frameworks after a fork. Dewey's worker does not run on Windows at all (it is a POSIX service: process groups, `SIGTERM`, a subreaper). |
| What can go wrong | Slow start on a small CPU; raise `startup_seconds`. | A connection, thread or native client created before the fork is shared with the children and misbehaves there: deadlocks, two processes on one socket, telemetry lost or duplicated. Anything the factory does to process-wide state (adding a log handler, configuring an exporter) runs once in the service and again in each task process, which already carries the service's copy: guard it with a check or let `dictConfig` replace rather than add. `atexit` hooks do not run in a forked service. |

By hosting:

| Where the worker runs | Pick |
|---|---|
| Linux containers priced by memory (ECS Fargate, Kubernetes, Fly, Render, Heroku-style dynos) | `fork`, once verified. Small CPUs and frequent scaling benefit most: a replacement container runs its first task in seconds and holds a quarter of the memory. |
| macOS (development laptops) | `spawn`. |
| Big VMs or bare metal with plenty of RAM | Either. `fork` still gives faster restarts and recycles. |
| Windows | Dewey's worker does not run there; `dewey run` refuses to start outside POSIX. |

Fork-readiness checklist:

- The process a child is forked from opens no database connection and starts no
  thread at import: the service's factory with the plain CLI, and with
  `manage.py dewey run` the launching process as well. `manage.py dewey run`
  qualifies: the system checks run without connecting. Module-level
  `engine.connect()`, a cache client that connects on import, or a background
  scheduler started in `AppConfig.ready()` do not.
- Telemetry exporters re-initialise after a fork. The OpenTelemetry SDK's batch
  processor and Logfire's exporter both register `os.register_at_fork` hooks and
  restart their threads in the child; Sentry's transport restarts per pid. A custom
  exporter with its own thread needs the same.
- Network clients (boto3 sessions and clients, `requests`/`httpx` sessions, gRPC
  channels) are created inside the task process or lazily on first use, never at
  module import. gRPC channels are not fork-safe, and neither are some native thread
  pools (OpenBLAS, torch): create those in the handler, or stay on `spawn`.
- Verify: run the worker with `fork` in staging under a representative load and
  compare what is delivered (log records, spans, error-tracker events, alert hook
  calls) with the same run on `spawn`. Both counts must match, per task.

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
  bounded: the service reports progress as it imports the application, checks the
  ledger and starts each task process, and `startup_seconds` without progress is a
  fault. A stalled parent's progress protocol then has an independent deadline,
  `supervision_seconds`. A live process alone is not proof of healthy ledger
  supervision.
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
