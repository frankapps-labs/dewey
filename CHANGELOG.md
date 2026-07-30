# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - Unreleased

First published release. `0.3.0` was skipped: the `dewey` name on PyPI already
carries a `0.3.0` from an unrelated 2011 project, and PyPI never allows a version
string to be reused.

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
- Renamed the scheduling column and Python attribute `process_after` → `scheduled_for`
  across SQLAlchemy models, Django models, dataclasses (`TaskEntry`,
  `NotificationEntry`), executor / sweep / query kwargs, and partial indexes
  (`ix_task_entries_pending_scheduled_for`, `ix_task_entries_failed_scheduled_for`,
  `ix_notif_pending_scheduled_for`, `ix_notif_failed_scheduled_for`). Pre-release
  setups using `Base.metadata.create_all` or fresh `makemigrations` are unaffected;
  the rename predates any tagged release.

### Added
- `dewey.adapters.DispatcherAdapter` protocol (`@runtime_checkable`) for the
  upcoming Dewey-driven dispatch model: `register(process_fn)` +
  `dispatch(task_id)`. Existing Huey/Celery adapters still expose the legacy
  `setup` / `enqueue` API and will gain conformance in a follow-up.
- Contract tests for `DispatcherAdapter`: runtime `isinstance` check, negative
  cases for partial conformers, and `inspect.signature` locks on both methods.
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
