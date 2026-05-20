# Dewey publish/use-readiness plan

Status legend: `[ ]` not started, `[~]` in progress, `[x]` done.

## Phase 0 — Decisions and naming

- [x] Package name is final: `dewey`.
- [x] Local repo folder renamed from `taskledger` to `dewey`.
- [x] Update docs and package metadata from taskledger to Dewey.
- [x] Keep framework support modular via extras:
  - `dewey` = core only
  - `dewey[sqlalchemy]` = SQLAlchemy sync models/executors
  - `dewey[async]` = SQLAlchemy asyncio support
  - `dewey[django]` = Django ORM integration
  - `dewey[huey]` = Huey adapter
  - `dewey[celery]` = Celery adapter
- [x] No `fastapi` extra unless Dewey imports FastAPI directly. FastAPI usage is `dewey[sqlalchemy,async]` plus docs.
- [x] Switch type checker from mypy to basedpyright for Dewey. Real SQLAlchemy `Sequence` annotations fixed; Django ORM dynamics suppressed via per-directory executionEnvironment (intractable without django-stubs, and only affects Dewey's internal Django code — consumers' Django code is unaffected). `uv run basedpyright` reports 0 errors.
- [x] basedpyright is the default type checker for Dewey.
- [x] Keep public releases on `0.x` until a real consumer validates the API. Flip to `1.0` when the API is proven in at least one real consumer and CI/release automation is complete.

## Phase 1 — Rename/package hygiene

- [x] Update stale `taskledger` references in current public docs and package metadata.
- [x] Fix `MANIFEST.in` path: `src/taskledger` → `src/dewey`.
- [x] Update GitHub workflows from pip/dev-extra assumptions to uv dependency groups.
- [x] Fix coverage target from `taskledger` to `dewey`.
- [x] Normalize test database naming to `dewey_test`.
- [x] Modernize license metadata to SPDX form: `license = "MIT"`.
- [x] Run `uv build` without warnings.
- [x] Run `twine check dist/*`.

## Phase 2 — Lint/typecheck/CI

- [x] Fix current ruff failures (126 → 0; merged async_conftest into conftest, added per-file E402 noqa to Django tests, autofixed UP017/C401/C408/B007, silenced UP042 on enums to preserve `str()` repr).
- [x] Add `make typecheck` using basedpyright if adopted.
- [x] CI matrix: Python 3.11, 3.12, 3.13.
- [x] CI commands use uv consistently.
- [x] CI runs lint, format check, typecheck, tests, build.

## Phase 3 — Local integration infra

- [x] Remove the repo-local FastAPI example to keep the published package lean; future integration docs/tests should live outside the package tree or as installed-wheel smoke tests.
- [ ] Add `docker-compose.yml` at repo root with Postgres and Redis for full test matrix.
- [ ] Add Huey worker/example path for integration testing.
- [ ] Add Celery worker/beat placeholders or optional profile.
- [ ] Add Make targets: `up`, `down`, `test-db`, `test-integration`.
- [ ] Document local setup and env vars.

## Phase 4 — Django production readiness

- [ ] Add initial Django migrations for task and notification models.
- [ ] Verify `python manage.py migrate` in a minimal Django app.
- [ ] Smoke-test `dewey[django]` from built wheel.
- [ ] Defer Django admin and management commands unless needed before 1.0.

## Phase 5 — Changelog/version polish

- [ ] Update changelog for current 0.2.0 work: async SQLAlchemy, JSONB→JSON, Django, Celery, notifications, pluggable backoff, trace context, current test count/coverage.
- [ ] Decide next development version after cleanup (`0.2.x`, `0.3.0-dev`, or leave `0.2.0` until tag).
- [ ] Define 1.0 release gate in README/changelog notes.

## Phase 6 — Automated smoke tests

- [ ] Build wheel in CI.
- [ ] Install wheel into a fresh env.
- [ ] Smoke-import core modules and extras modules.
- [ ] Optional installed-wheel Postgres task create/process smoke test.

## Phase 7 — Free OSS tooling/security

- [ ] Coveralls coverage upload.
- [ ] GitHub CodeQL for Python.
- [ ] Dependency audit (`pip-audit` or equivalent).
- [ ] Evaluate Snyk free OSS integration once the public repo is settled.
- [ ] Optional later: Dependabot/Renovate and OpenSSF Scorecard.

## Logfire / structured-logging extra (future)

- [ ] `dewey[logfire]` extra that auto-configures the `TraceContextFilter` and
      a JSON formatter so `dewey.*` logs ship straight to Pydantic Logfire
      (or any OTLP backend) with `dewey_request_id` flowing as a span attribute.
- [ ] Optional `dewey[otel]` extra: capture the current OTel span at `create_task`,
      restore the context at `process_task`, create spans for Dewey's own work.
- Rationale: Dewey shipping a one-line opt-in would make structured logging
      adoption trivial. Commercial path (Logfire paid / hosted / self-hosted
      SigNoz/Loki) keeps OTLP swap path clean.
- Defer until: a real consumer wants it, or revenue starts (whichever first).

## Resource isolation contract (future, before production docs)

- [ ] Add an explicit production `resource_profile` / deployment-profile setting.
      Dewey should fail closed outside test/dev mode unless the app chooses a
      supported profile such as `cohabiting` or `dedicated-db`.
- [ ] Document Postgres-first semantics: Postgres is the durable ledger and
      source of truth; Redis/RabbitMQ/Postgres NOTIFY are optional wake-up
      accelerators only. Lost broker messages must be harmless.
- [ ] Document FastAPI cohabiting setup: separate SQLAlchemy engine/session
      factory for Dewey, bounded pool, `max_overflow=0`, `pool_timeout`,
      statement/lock/idle transaction timeouts, and ideally a separate DB role
      with a connection limit.
- [ ] Document Django future setup: separate `DATABASES["dewey"]` alias and
      database router, even when pointing at the same physical Postgres DB.

## Wake-on-insert in Dewey core (LISTEN/NOTIFY)

Motivation: under a poll loop, a worker only sees new rows on the next poll
interval (and only as much as its claim batch allows per tick). High-priority
arrivals therefore wait for an in-flight batch to drain before being seen.
Batch-size and tight-loop tuning can trade priority lag for throughput, but
those band-aids can also increase application/database contention under
sustained insert pressure.

- [x] Decide the shape: `dewey.sqlalchemy.listen` exposes transactional
      `notify_work_available(_async)` producers plus
      `AsyncPostgresWorkListener`, a dedicated asyncpg-backed listener that
      workers can idle on. It round-trips through the same `task_entries` /
      `notification_entries` tables so brokers stay optional.
- [x] Ship the helper behind the existing `dewey[async]` / `dewey[sqlalchemy]`
      extras; `dewey[async]` now includes asyncpg so LISTEN works without a
      new Dewey extra.
- [x] Keep the listener contract validated through library tests and future
      installed-wheel smoke tests.
- [ ] Re-run priority-lane and batch-pressure integration scenarios and
      re-baseline. Expectation: eager queue p95 closes toward handler runtime
      and accept-latency regressions from tight polling disappear because the
      worker no longer polls under load.

## Lifecycle hooks — latency payload (future)

- [ ] When lifecycle hooks land, the `task_completed` hook payload must
      include `queue_ms`, `handler_ms`, `end_to_end_ms` (derivable from
      `created_at` / `started_at` / `completed_at`). This is the durable
      surface for runtime latency telemetry; integration tests can use window
      SQL until then.
- [ ] Add partial index on `task_entries(completed_at) WHERE completed_at IS
      NOT NULL` to keep windowed percentile queries cheap.
- [x] Fix SQLAlchemy executors to stamp `completed_at` at actual completion
      time, not claim time; latency gates depend on this.

## Correlation-context extraction (future)

- [ ] When a second consumer wants the trace-context primitives, extract the
      ~50 LOC of generic helpers
      (ContextVar + filter + get/set/reset) into a tiny shared package such as
      `correlation-context`. Dewey then keeps only the metadata-round-trip glue.
- Defer until: 2nd consumer materialises.

## Phase 8 — First real consumer

- [ ] Wire Dewey into a real async SQLAlchemy consumer.
- [ ] Keep the task queue/broker as transport and Dewey/Postgres as guarantee.
- [ ] Feed integration rough edges back into Dewey before public release.
- [ ] Tag/publish `0.2.x` once the first real integration is clean.
- [ ] Promote to `1.0` only after real usage proves the API stable.
