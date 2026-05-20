# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
