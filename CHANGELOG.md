# Changelog

This changelog records the public Frankapps Dewey release lineage. Earlier versions under
the `dewey` package name predate this lineage and are intentionally not represented here.

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

[0.5.1]: https://github.com/frankapps-labs/dewey/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/frankapps-labs/dewey/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/frankapps-labs/dewey/releases/tag/v0.4.0
