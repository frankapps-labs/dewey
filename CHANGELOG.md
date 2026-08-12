# Changelog

This changelog records the public Frankapps Dewey release lineage. Earlier versions under
the `dewey` package name predate this lineage and are intentionally not represented here.

## [0.5.1]

### Fixed

- Django, SQLAlchemy sync, and SQLAlchemy async task creation now consistently reject a
  naive `scheduled_for` value with
  `ValueError("scheduled_for must be a timezone-aware datetime")` before writing a task row
  or sending a work-available notification. Timezone-aware values and `None` remain valid.
  This intentionally turns an input accepted by 0.5.0 into an immediate error rather than
  letting PostgreSQL interpret an ambiguous wall-clock time using its session timezone.

## [0.5.0]

### Added

- First-class optional Django/Huey integration through `dewey.contrib.django_huey`, with
  separate producer, dispatcher, and worker database aliases.
- Absolute task expiry through `expires_at`, the terminal `EXPIRED` state, expiry audit
  timestamps, and matching Django and SQLAlchemy schema support.
- Race-safe idempotent creation through `create_or_get_task()` for Django and SQLAlchemy
  sync/async producers.
- Database-backed dispatcher heartbeats, readiness queries and checks, and the
  `dewey_doctor` Django management command.

### Changed

- Due failed tasks became directly claimable, and dispatcher pacing accounts for retries,
  scheduled work, and deadlines.
- Expiry is evaluated after the worker acquires the task-row lock and before the handler
  runs or consumes an attempt.
- Idempotent creation requires timezone-aware `scheduled_for` values.

## [0.4.0]

### Added

- The first public Frankapps Dewey release: a PostgreSQL-backed durable task ledger with
  SQLAlchemy sync/async and Django producer and worker APIs.
- Cooperative dispatchers using `FOR UPDATE SKIP LOCKED`, recoverable `DISPATCHING` state,
  PostgreSQL LISTEN/NOTIFY wake-up with polling fallback, and recovery sweeps.
- Policy-driven retry, backoff, queue, priority, and attempt budgets, with typed retry and
  non-retryable errors.
- A Huey transport adapter that carries task IDs while Dewey retains retry authority.
- Django migrations and dispatcher management command, query/action APIs, and an
  installed-wheel PostgreSQL/Redis/Huey smoke test.

[0.5.1]: https://github.com/frankapps-labs/dewey/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/frankapps-labs/dewey/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/frankapps-labs/dewey/releases/tag/v0.4.0
