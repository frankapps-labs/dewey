# Changelog

This changelog records the public Frankapps Dewey release lineage. Earlier versions under
the `dewey` package name predate this lineage and are intentionally not represented here.

## [0.7.0] - 2026-11-15

A drop-in upgrade from 0.6: no migration, no setting to change; `spawn` stays the
default start method and tracing stays off until asked for. One log message
changed: the inline executors' "Task dead-lettered ..." is now "Task dead ...", the
same record the worker emits. Match failures on the record's `outcome` and `reason`
fields rather than on the message text.

### Fixed

- **Workers start on small CPUs.** Task processes start one after another and the
  worker claims as soon as the first is ready; `startup_seconds` bounds the
  application import and each task process separately, measured from its own start,
  and the supervisor paces startup by progress instead of one deadline from launch.
  A worker with eight slots on 0.25 vCPU used to exit 70 before running a task; it
  now runs its first task in about 9 s. Errors name `startup_seconds`.
- On macOS the supervisor reported exit 71 instead of 70 when the service's process
  group held only zombies.
- **Every failed attempt is logged.** A handler that raised was recorded on its row
  and reported to the alert hook, but no log record reached an error tracker. The
  worker now logs each failed attempt on `dewey.worker`: WARNING while the task
  retries, ERROR once it is dead, naming the exception's type, with the message and
  `exc_info` as well under the new `failure_details: "full"`
  (`DEWEY["FAILURE_DETAILS"]`; the default `"type"` keeps handler input and SQL
  parameters an exception may quote out of the log pipeline), and `task_id`,
  `task_type`, `queue`, `attempt`, `max_attempts`, `outcome` and `reason` as
  structured fields. Attempts recovered
  after a worker death or a time limit are logged by the recovering worker. The
  record is sent after the settlement commits, like the alert, so a settlement the
  database budget rolls back and retries is logged once; it goes out before the
  alert hook, and the hook runs in a copy of the context so nothing it attaches can
  silence the worker's own records or spans. Every value on the record is a string,
  a number or None (the exception travels as `exc_info` only), so a JSON log handler
  never has to find an encoder for it: Logfire imports pandas and numpy to look for
  one, which on a small CPU held the task process long enough for the worker to
  replace it before the attempt's span and record went out. The inline
  `process_task` helpers log the same record on their own logger.
- `process_task` (SQLAlchemy sync and async, Django) binds `dewey.current_task()`
  with the policy's `statement_timeout_s`/`lock_timeout_s`, and the Django one
  applies them to the connection the way the worker does, so a handler tested inline
  runs under its production budgets.

### Added

- `start_method: "fork"` (`DEWEY["START_METHOD"]`): import the application once and
  fork the task processes from the process that holds it: the service with the
  plain CLI (it imports the application after the supervisor forked it), or the
  host itself with `manage.py dewey run` (Django is already imported there).
  Processes share the imported application copy-on-write (a six-slot Django worker
  under `manage.py dewey run`: 129 MB instead of 438 MB of PSS; first task in a
  second instead of four) and task processes start in milliseconds. The worker
  closes its connections before every fork, blocks the stop signals across each
  fork so a stop in the window is not lost, and children reset inherited signal
  handlers; a forked service shuts logging down before it exits. Opt-in: the
  process a child is forked from must be fork-safe. See
  [choosing a start method](docs/native-worker.md#choosing-a-start-method) in the
  worker docs for when to pick which, and the readiness checklist.
- Django settings `START_METHOD`, `STARTUP_SECONDS`, `SUPERVISION_SECONDS`,
  `SHUTDOWN_SECONDS` and `MAX_RESTARTS`.
- **OpenTelemetry tracing**, opt-in: `pip install 'dewey[otel]'` and
  `tracing: true` (`DEWEY["TRACING"] = True`). Each attempt a task process runs is a
  `CONSUMER` span named `dewey task <type>`, a child of the span that created the
  task (the row already carries its `traceparent`) or the root of a new trace for a
  schedule tick, with `messaging.system = "dewey"` and the task id, type, queue,
  attempt and max attempts as attributes. It is the current span while the handler
  runs, so the handler's own spans and instrumented libraries nest under it, and it
  ends with the exception recorded and error status when the handler raised. The
  task process flushes the provider when the worker stops it. Dewey depends on
  `opentelemetry-api` only; the application configures the SDK or vendor (Logfire,
  Sentry) that exports, in the worker process as in the web process. Works with
  `spawn` and `fork`. A worker told to trace without the API refuses to start.
- `execution_timeout_s` in the task policy: `@dewey.task(..., execution_timeout_s=90)`
  or `configure_policies`. Like `max_attempts` it is stamped onto the row by
  `create_task`, `create_or_get_task` (the caller's own value still wins and is
  part of the idempotent-creation contract) and schedule ticks, from the policy
  resolved when the schedule was applied. The worker's `task_timeout_seconds` stays
  the ceiling for a larger value. A schedule definition carries the field only when
  set, so already-applied schedules report no drift after upgrading.
- `dewey.current_task_or_none()`: `None` outside a handler, for handlers that are
  also called directly.
- Upgrade notes: `create_task` returns a record (use `.id`, not `.pk`), and how to
  replace the auto-registered `TaskEntry` admin.

## [0.6.0] - 2026-11-15

Dewey now owns the runner and the clock as well as the ledger. One process type,
`dewey run`, claims, executes, recovers and creates schedule ticks; there is no broker
and no dispatcher. This is a breaking release: read
[upgrading to 0.6](docs/upgrading-0.6.md) first.

### Added

- **The worker.** `dewey run --app module:factory` (`python manage.py dewey run` for
  Django, with no factory) runs tasks from Postgres with one process per task, sync or
  async handlers, on Django or SQLAlchemy sync/async storage. Claims are leases with
  execution tokens: a worker that dies loses its leases and another worker retries the
  work; a stale execution cannot overwrite a newer outcome. SIGTERM drains within
  `shutdown_seconds`. Only the application factory is required; every limit has a
  default sized from the concurrency. Workers claim only task types they have handlers
  for, write a heartbeat, and must run inside a container or equivalent kill-all
  boundary.
- **Schedules.** `@dewey.task(..., schedule=dewey.cron("15 3 * * *", tz="Europe/London"))`
  and `dewey.schedule(name, task=..., when=dewey.every(minutes=5), ...)` declare
  schedules in code under their own name. Every tick becomes one ordinary task row,
  created exactly once however many workers run, with the key `schedule:<name>:<tick>`.
  Per schedule: `catch_up` (`once`, `skip`, `all`), `overlap` (`skip`, `allow`) and
  `expire` (`next_tick`, `None`). The cron parser is dependency-free; wall-clock
  schedules fire once across a daylight-saving gap or repeat, and all "is it due"
  decisions use the database clock.
- **Plan and apply.** `dewey schedules plan` shows what the code would change in the
  schedule table (and exits 2 when it would); `dewey schedules apply` is the only
  writer of definitions. Removal expires a schedule's unstarted ticks unless
  `--keep-pending`. Workers report drift at startup; `auto_apply_schedules` lets a
  single-node worker apply instead. `dewey schedules pause [--for 10m]` and `resume`.
- **The `dewey` command and operations API.** `queues`, `tasks list|show|retry|kill`,
  `dlq list|retry`, `workers`, `schedules ...` and `doctor`, each with `--json`.
  `dewey.ops` exposes the same operations by name for Django and SQLAlchemy, returning
  plain JSON-safe data. Arguments and error text are hidden unless revealed.
- **Time limits.** Per-task `execution_timeout_s`, enforced by stopping the task
  process; `dewey.current_task().deadline` and `.remaining()`; per-task
  `statement_timeout_s` and `lock_timeout_s` in the task policy, applied to the Django
  worker connection and available to SQLAlchemy handlers through
  `apply_task_timeouts()`.
- **Alerts.** `dewey.on_alert(hook)` (or `alert_hook="module:function"`) receives
  `task_dead`, `execution_recovered`, `schedule_tick_missed` and
  `worker_heartbeat_stale` events after the causing transaction commits.
- **Trace context.** Task creation captures the ambient trace context, and an
  OpenTelemetry `traceparent` when one is active; both are restored around the handler
  and included in the pull-runner invocation. OpenTelemetry stays optional.
- **Pull runners.** `dewey.runner` with `SQLAlchemyRunner`, `AsyncSQLAlchemyRunner`
  and `DjangoRunner` lets a host gateway serve workers in other languages on the same
  ledger: claim, start, renew, complete, fail, release, cooperative `request_cancel`,
  `recover` and `tick`, inside the host's own transaction. Candidate protocol 1.
- **Task context.** `dewey.current_task()` gives handlers their identity and stable
  `effect_key(name)` values for deduplicating external effects across retries.
- **Task history and retention.** Transactional outcome events per task, and an
  optional, experimental Django admin overview with a diagnosed single retry and
  completed-task retention controls. See [the admin overview](docs/dashboard.md).

### Changed

- Workers claim from a new index, `ix_task_claim`, one queue at a time, so the cost of
  a claim no longer grows with the backlog.
- A database too slow for `db_wait_seconds` pauses a worker instead of stopping it;
  it stops only once its leases can no longer be trusted.
- One maintenance round creates ticks for as many due schedules as fit in about one
  database budget, not for a single batch of 20.
- A task process that has just finished is no longer stopped and replaced when its
  lease renewal races the report of its result.
- A task whose type no worker has a handler for is now left unclaimed, not failed.
- `process_task` is a test and eager-execution helper only; nothing recovers a row it
  leaves behind.
- Django settings are the worker's: `DATABASE`, `QUEUES`, `CONCURRENCY`,
  `AUTODISCOVER`, `AUTO_APPLY_SCHEDULES`, `ALERT_HOOK`, `TIMING`. Unknown keys,
  including the old dispatcher ones, are an error.
- `get_dispatchers` is `get_workers`; the heartbeat types are `WorkerHeartbeat`. The
  table keeps its name.
- `manage.py dewey_doctor` is `manage.py dewey doctor`.

### Removed

- The Huey adapter, `dewey.adapters`, `dewey.contrib.django_huey` and the `huey` extra.
- The dispatcher, its backends, `manage.py dewey_dispatcher` and LISTEN/NOTIFY
  wake-ups. Task creation no longer issues `NOTIFY`.
- The sweep functions; workers recover their own leases.

### Upgrade

- Stop the old dispatcher and broker workers, then migrate. Django migrations
  `0003`-`0010` add ownership, history, retention, budgets, schedules and the claim
  index, and `0008`
  hands back rows the removed path left claimed. SQLAlchemy users apply the additive
  SQL in [execution ownership](docs/execution-ownership.md) and
  [upgrading to 0.6](docs/upgrading-0.6.md) and call `reset_legacy_claims()` once.

## [0.5.3] - 2026-09-19

### Security

- Bump the supported dependency floors to CVE-fixed versions: Django 5.2.17 (and 6.0.8 on the
  Python 3.12+ seam) and sqlparse 0.6.0, addressing PYSEC-2026-3717 (Django) and
  PYSEC-2026-3696/3697/3698/3699/3923 (sqlparse).

## [0.5.2] - 2026-08-12

### Fixed

- The `django` extra now permits Django 5.2 on Python 3.12 and newer by declaring
  `Django>=5.2.16,<7` without a Python-version split. Applications that intentionally
  remain on the Django 5.2 LTS line can install Dewey alongside Huey 3, while unconstrained
  Python 3.12+ environments may still resolve Django 6.

## [0.5.1] - 2026-08-12

### Fixed

- Django, SQLAlchemy sync, and SQLAlchemy async task creation now consistently reject a
  naive `scheduled_for` value with
  `ValueError("scheduled_for must be a timezone-aware datetime")` before writing a task row
  or sending a work-available notification. Timezone-aware values and `None` remain valid.
  This intentionally turns an input accepted by 0.5.0 into an immediate error rather than
  letting PostgreSQL interpret an ambiguous wall-clock time using its session timezone.

## [0.5.0] - 2026-08-12

### Added

- First-class optional Django/Huey integration through `dewey.contrib.django_huey`, using
  `djhuey.HUEY`, one retry-disabled Dewey processor, `close_db`, and an importable
  module-level `dispatch` callable.
- Independent `DEWEY["WORKER_DATABASE"]` configuration so producers, dispatchers, and
  workers can use explicit database aliases without losing producer transactionality.
- Absolute task deadlines through `expires_at`, the terminal `EXPIRED` state,
  `expired_at` audit timestamps, Django migration `0002`, and matching SQLAlchemy metadata.
- Expiry enforcement before dispatch and again after a worker acquires the task-row lock,
  without invoking the handler or consuming an attempt.
- Race-safe `create_or_get_task()` APIs for Django and SQLAlchemy sync/async producers.
  Identical creation contracts return the existing task in any lifecycle state; conflicting
  reuse raises a redacted `IdempotencyConflictError` without aborting the caller's outer
  transaction.
- Database-backed dispatcher heartbeats, freshness and queue-coverage queries, and
  lightweight Django system checks.
- `python manage.py dewey_doctor` with human-readable and stable JSON output for validating
  settings, PostgreSQL schema and migrations, dispatch wiring, handler registration shape,
  and fresh dispatcher coverage.

### Changed

- Due `FAILED` rows are directly claimable, so ordinary retry timing no longer depends on
  the recovery sweep interval.
- Dispatcher pacing accounts for the earliest retry, scheduled task, and deadline while
  retaining polling as the correctness path.
- Expiry is evaluated using the time observed after the worker acquires the row lock, so
  lock contention cannot allow already-expired work to start.
- Idempotent creation requires timezone-aware `scheduled_for` values for stable database
  round trips.
- Django producer guidance now separates producer, dispatcher, and worker database roles.
  Producer task creation must use the same alias and transaction as its business write.
- `DEWEY["DISPATCH"]` must resolve to a module-level callable importable by Django;
  traversal through an object such as `myapp.tasks.adapter.dispatch` is not supported.

### Fixed

- Django task creation, processing, sweeps, retries, kills, and transactional notifications
  consistently use the resolved database alias, avoiding cross-connection writes and
  `TransactionManagementError` in routed multi-database projects.
- Django `completed_at` records when the handler actually completes rather than when its row
  is claimed.
- Claim ordering combines immediate and due scheduled/retried work by effective due time,
  preventing steady producer traffic from starving due retries.
- Recovery sweeps continue while a dispatcher retries an unwritable stranded release, so a
  partial database failure cannot stall timeout recovery in a single-dispatcher deployment.

### Compatibility

- Existing task tables require the additive deadline, schedule-snapshot, and heartbeat
  schema from Django migration `0002` or an equivalent Alembic/SQL migration;
  `Base.metadata.create_all()` does not alter an existing SQLAlchemy table.
- Existing rows receive nullable deadline fields and do not expire. Status consumers must
  accept `expired`; reversing the Django migration maps 0.5-only `EXPIRED` rows to `DEAD`.
- `create_task()` retains its existing duplicate-key behavior; producer idempotency is
  opt-in through `create_or_get_task()`.
- Custom dispatch backends may add `next_due()` and `heartbeat()` for improved pacing and
  readiness reporting. Both capabilities remain optional and retain safe fallbacks.

## [0.4.0] - 2026-08-11

First public Frankapps Dewey release. Version 0.3.0 was skipped because the `dewey` name on
PyPI already contains an unrelated historical 0.3.0 release.

### Added

- A PostgreSQL-backed durable task ledger for Django and SQLAlchemy sync/async applications,
  with explicit task states, attempts, errors, timestamps, queues, priorities, schedules,
  and correlation metadata.
- Transactional task creation: the task row and PostgreSQL `NOTIFY` commit together, so a
  rolled-back business transaction creates no work and a committed task remains recoverable.
- Cooperative sync and async dispatchers using `FOR UPDATE SKIP LOCKED`, allowing multiple
  instances to claim work safely without leader election.
- A visible `DISPATCHING` state and `dispatching_at` timestamp for the window between a
  database claim and worker start, plus timeout recovery for abandoned dispatches.
- PostgreSQL LISTEN/NOTIFY wake-up for psycopg2, psycopg3, and asyncpg, with polling retained
  to recover missed notifications and newly due scheduled work.
- `@dewey.task(...)` handler registration with ordinary directly-callable Python functions;
  workers resolve handlers and invoke persisted `args` and `kwargs` as a normal function
  call.
- Layered task policy through defaults, decorator declarations, and project configuration,
  covering queue, priority, attempt budget, backoff, retry classes, and fail-fast classes.
- Typed `TransientError`, `NonRetryableError`, and `RetryAfter` failure control. Dewey owns
  retries and applies `RetryAfter` only when it is later than the configured policy delay.
- JSON-safe argument serialization for datetime/date/time, UUID, Decimal, and Enum values,
  with explicit rejection of ORM instances, bytes, sets, non-string mapping keys,
  non-finite floats, and excessive nesting.
- Atomic worker claims from `PENDING` or `DISPATCHING`; duplicate transport delivery to an
  already-processing or terminal row is a logged no-op.
- Directly queryable backlog and action APIs for pending, dispatching, failed, and dead
  tasks, aggregate statistics, manual retry/kill, and retention purges.
- Recovery sweeps for due failures, abandoned `DISPATCHING` rows, and stuck `PROCESSING`
  rows, with configurable dispatch and processing thresholds.
- SQLAlchemy sync and async producer, worker, dispatch, sweep, and query implementations,
  including an async dispatcher for asyncpg-only deployments.
- Django models and an initial migration, lazy imports, validated `DEWEY` settings, routed
  database-alias support, transactional notifications, and the `dewey_dispatcher`
  management command.
- A Huey transport adapter that carries task IDs and registers workers with Huey retries
  disabled, leaving retry scheduling under Dewey's single authority.
- Independent database and transport backoff. Dispatchers survive database outages, retain
  claims whose release could not be written, and retry those releases before claiming new
  work rather than waiting for the dispatch-timeout sweep.
- Queue-aware claiming, priority ordering, delayed execution through `scheduled_for`, and
  policy-stamped attempt budgets that remain stable for in-flight work.
- Installed-wheel PostgreSQL/Redis/Huey smoke coverage plus multi-process claim tests for
  the packaged Django and SQLAlchemy paths.

[0.7.0]: https://github.com/frankapps-labs/dewey/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/frankapps-labs/dewey/compare/v0.5.3...v0.6.0
[0.5.3]: https://github.com/frankapps-labs/dewey/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/frankapps-labs/dewey/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/frankapps-labs/dewey/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/frankapps-labs/dewey/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/frankapps-labs/dewey/releases/tag/v0.4.0
