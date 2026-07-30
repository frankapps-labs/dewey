# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - Unreleased

First published release. `0.3.0` was skipped: the `dewey` name on PyPI already
carries a `0.3.0` from an unrelated 2011 project, and PyPI never allows a version
string to be reused.

### Deprecated
- The notification layer (`dewey.*.notifications`, `NotificationEntry`,
  `NotificationAttempt`, `Channel`, `ChannelRegistry`) is now documented as
  **experimental** and sits outside the stability expectations for the rest of the
  package. It is a second ledger with its own state machine and sweep, it predates the
  dispatcher, and it is not dispatcher-driven. The open question for a later release is
  whether a notification should simply be a task type with a channel handler, replacing
  the parallel retry engine with `TaskPolicy`. Migrations still ship for its tables.

### Removed
- Celery support: the `dewey.adapters.celery` module, the `[celery]` extra, and
  Celery from the package keywords. Huey is the only advertised transport for the
  first release. The pre-inversion (`enqueue`-era) Celery adapter is kept for
  reference on the `archive/celery-adapter-enqueue-era` branch; a Celery adapter
  can return in a later release against the `register`/`dispatch` contract.
- `dewey.adapters.BaseAdapter`, the legacy producer-side contract
  (`enqueue(task_id, queue, priority)` + `enqueue_sweep()`). Producers no longer
  talk to a broker at all, so the package now publishes exactly one adapter
  contract: `DispatcherAdapter`.

### Changed
- **Breaking:** task rows persist handler arguments as explicit `args` (list) and
  `kwargs` (dict) columns instead of a single `payload` dict, and handlers are now
  invoked as `handler(*args, **kwargs)` instead of `handler(task_type, payload)`.
  A task row now records a *function call* rather than a bag of data: handlers are
  ordinary Python functions, a wrong argument list fails with a plain `TypeError`
  before any side effect, and migrating a `f.delay(42)` / `f.delay(client_id=42)`
  call site leaves the handler body untouched.
- **Breaking:** `process_task` no longer requires a handler argument — it resolves the
  handler and the retry rules from the task type. Passing one explicitly still works and
  still wins. The failure decision now lives in `dewey.core.execution.classify_failure`,
  shared by the SQLAlchemy sync, SQLAlchemy async and Django paths so they cannot drift.
- **Breaking:** `create_task` fills `queue`, `priority` and `max_attempts` from the
  resolved policy when not given. The attempt budget is stamped on the row, so in-flight
  work keeps the budget it was created with when policy changes.
- Workers may claim from `PENDING` or `DISPATCHING`. A delivery for a row that is already
  processing or terminal is a logged no-op rather than an error, which is what makes
  duplicate transport delivery harmless.
- An unknown task type is an ordinary failed attempt, not a crash: a worker deployed
  before the handler retries with backoff and only dead-letters once the budget is spent.
- `dewey.django` lazy attribute access is cached, fixing `dewey.django.sweep` resolving to
  the function on first access and to the submodule afterwards. Names are also re-exported
  under `TYPE_CHECKING` for IDEs and type checkers.
- Django model defaults are module-level callables so migrations can serialise them, and
  Django index names no longer carry the pre-rename `process_after` suffix.
- Renamed the scheduling column and Python attribute `process_after` → `scheduled_for`
  across SQLAlchemy models, Django models, dataclasses (`TaskEntry`,
  `NotificationEntry`), executor / sweep / query kwargs, and partial indexes
  (`ix_task_entries_pending_scheduled_for`, `ix_task_entries_failed_scheduled_for`,
  `ix_notif_pending_scheduled_for`, `ix_notif_failed_scheduled_for`). Pre-release
  setups using `Base.metadata.create_all` or fresh `makemigrations` are unaffected;
  the rename predates any tagged release.

### Added
- **Dewey-driven dispatch.** `dewey.dispatcher.Dispatcher` claims ready rows with
  `FOR UPDATE SKIP LOCKED`, commits them as `DISPATCHING`, and hands task IDs to a
  transport. Any number of dispatchers cooperate without a leader election.
  Backends: `dewey.sqlalchemy.dispatch.SQLAlchemyDispatchBackend` and
  `dewey.django.dispatch.DjangoDispatchBackend`.
- `dewey.dispatcher.AsyncDispatcher` and
  `dewey.sqlalchemy.dispatch.AsyncSQLAlchemyDispatchBackend`, so an asyncpg-only
  deployment does not have to add a synchronous driver and a second engine to run a
  dispatcher. Pacing decisions (transport backoff, sweep interval) are shared code with
  the sync loop so the two cannot drift; `dispatch_fn` may be sync or a coroutine
  function; wake-up uses the asyncpg listener rather than a blocking thread.
- `DISPATCHING` state and `dispatching_at`, with a partial index. Makes the window
  between "handed to the broker" and "a worker started it" visible, so a dispatcher
  that dies mid-dispatch leaves a recoverable row rather than lost work.
- `sweep_dispatching` reclaims rows past `dispatch_timeout_seconds` (default 300),
  wired into `sweep()` on all three backends alongside the existing failed and
  stuck passes.
- **Task policy as data.** `TaskPolicy`, `TASK_DEFAULTS`, `Constant` / `Exponential`
  / `Custom` backoff, a process-local registry, and a resolver merging three layers:
  `TASK_DEFAULTS` < `@dewey.task(...)` < `configure_policies(...)`. Tier 1 fields
  only: `queue`, `priority`, `max_attempts`, `backoff`, `retry_on`, `fail_fast_on`.
  Unknown fields and invalid values raise at declaration time, so `max_attemps=3`
  fails at import instead of silently doing nothing.
- `@dewey.task("type", **policy)` registers a handler and returns it untouched — it
  stays an ordinary, directly callable, directly testable function, with no
  `.delay()` to re-couple producers to worker imports.
- Typed errors: `TransientError`, `NonRetryableError`, `RetryAfter`, plus
  `SerializationError`, `DuplicateTaskTypeError`, `UnknownTaskTypeError`.
  `RetryAfter(n)` schedules at `max(n, policy_backoff)`, so a handler can ask for
  later than policy but a misbehaving provider cannot pull retries inside your rate
  budget. Anything outside `retry_on` dead-letters at once rather than burning the
  remaining budget.
- Argument serialization for `datetime`, `date`, `time`, `UUID`, `Decimal` and
  `Enum` (to strings, keeping rows readable and the encoding language-neutral).
  `bytes`, ORM instances, sets, non-string mapping keys, non-finite floats and deep
  nesting are refused with a message naming the offending position.
- **Django production readiness:** initial migrations ship in the wheel,
  `manage.py dewey_dispatcher` (with `--once`, `--queues`, `--idle-poll`), a
  validated `DEWEY` settings dict that rejects misspelled keys, and transactional
  NOTIFY — Postgres holds notifications until commit, so a rolled-back producer
  cannot wake a dispatcher and no `on_commit` bookkeeping is needed.
- LISTEN wake-up for synchronous dispatchers on both psycopg2 and psycopg3, with
  automatic fallback to polling. Polling remains the correctness path.
- `get_dispatching()` query on all three backends; `get_stats()` now reports the
  `dispatching` count.
- `HueyAdapter.register(process_fn)` / `dispatch(task_id)`, registered with
  `retries=0` so Dewey keeps sole authority over retry scheduling.
- `dewey.adapters.DispatcherAdapter` protocol (`@runtime_checkable`) with contract
  tests: runtime `isinstance` check, negative cases for partial conformers, and
  `inspect.signature` locks on both methods.
- Local test infrastructure: `docker-compose.yml` (Postgres + Redis on offset
  ports) and `make up / down / test-integration / wheel-smoke`.
- An installed-wheel smoke test that installs the built wheel into a clean
  virtualenv and runs migrate, dispatch, process, duplicate delivery, rolled-back
  producer, Redis outage and recovery, retry-then-dead-letter, and a real Redis
  round trip through a Huey worker.
- Real-Postgres multi-process concurrency harness
  (`tests/_helpers/concurrency.py`) exercising `FOR UPDATE SKIP LOCKED` with
  uuid-suffixed tables, queue-drain-before-join, and child-side tracebacks
  surfaced into the parent.

## [0.2.0] - 2026-04-23

### Added
- Notification layer with event registry, channel protocol, and per-attempt tracking
- `NotificationStatus` state machine (pending → sending → sent/failed/dead)
- `Channel` protocol and `ChannelRegistry` for mapping events to channels
- `NotificationEntryModel` + `NotificationAttemptModel` (SQLAlchemy)
- `NotificationEntry` + `NotificationAttempt` Django models
- Notification executor: `create_notification`, `send_notification`, `process_notification`
- `create_notifications_for_event` — registry-driven multi-channel dispatch
- Notification sweep: `sweep_failed_notifications`, `sweep_stuck_notifications`
- Notification queries: stats, pending, failed, dead, per-task, retry, kill, purge
- Celery adapter with native queue routing and priority support (`dewey.adapters.celery`)
- Beat schedule auto-registration for periodic sweep
- Django adapter: models, executor, sweep, queries (`dewey.django`)
- Django AppConfig (`"dewey.django"` in INSTALLED_APPS)
- `transaction.atomic` on `process_task`, `retry_task`, `kill_task` (Django)
- Lazy imports in `dewey.django.__init__` to avoid AppRegistryNotReady
- `pytest-django` + Django test suite (32 tests)
- Full test suite now at 94 tests, 83% coverage

## [0.1.0] - 2026-04-21

### Added
- Core state machine (`TaskStatus`, transitions, `should_retry`, `should_die`)
- Exponential backoff with configurable cap
- Pure Python `TaskEntry` dataclass
- SQLAlchemy models (`TaskEntryModel`) with Postgres partial indexes
- Task executor (`create_task`, `process_task`) with SELECT FOR UPDATE
- Sweep module (failed retry, stuck task recovery)
- Query & action API (stats, get_pending/failed/dead, retry, kill, purge)
- Huey adapter with periodic sweep registration
- Full test suite (core + SQLAlchemy + sweep + queries)
