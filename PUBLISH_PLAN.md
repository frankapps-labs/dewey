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
  - (`dewey[celery]` removed for 0.4.0 — Huey is the only advertised transport)
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
- [x] CI adds a Redis service, explicit Django 4.2 and 5.1 matrix entries, and an
      installed-wheel smoke job.

## Phase 3 — First-release architecture: dispatcher

Dewey's first public release is now shaped around Dewey-owned dispatch:
Postgres remains the durable control plane and broker integrations become
worker-pool transports.

- [x] Introduce the `DispatcherAdapter` protocol (`register(process_fn)` +
      `dispatch(task_id)`) and contract tests.
- [x] Huey conforms to `DispatcherAdapter` (`register`/`dispatch`, `retries=0`).
      Celery is removed from this release; the pre-inversion adapter is archived on
      the `archive/celery-adapter-enqueue-era` branch.
- [x] Dewey dispatcher loop: claim with `FOR UPDATE SKIP LOCKED`, commit as
      `DISPATCHING`, call `adapter.dispatch(task_id)`, reclaim abandoned
      dispatch/processing rows, and run the sweep tick.
- [x] Dispatch wake-up via LISTEN on psycopg2 and psycopg3, with polling as the
      correctness path rather than a fallback.
- [x] Tests for claim/dispatch/reclaim, including multi-dispatcher concurrency
      against real Postgres and transport-failure release.
- [x] Documented the broker relationship: transports carry task IDs; Postgres is the
      durable source of truth (`docs/adapters.md`, `docs/concepts.md`).

## Phase 4 — First-release architecture: TaskPolicy chain

Task behavior should be declared in a Dewey policy registry, not scattered
across broker decorators. Producers create task rows by type; handlers stay
small, bounded, and dumb.

- [x] `TaskPolicy`, `Constant`/`Exponential`/`Custom` backoff, process-local
      registry, and resolver.
- [x] `@dewey.task(...)` registers the handler and its policy, returning the
      function untouched.
- [x] Producer API: `create_task(task_type, args=..., kwargs=..., ...)`, with
      arguments persisted as explicit `args`/`kwargs` columns.
- [x] Typed errors: `TransientError`, `NonRetryableError`, `RetryAfter(n)` floored at
      the policy backoff.
- [x] Precedence: `TASK_DEFAULTS` < decorator < `configure_policies(...)`. DB runtime
      overrides remain a documented future boundary.
- [ ] Tier-1 lifecycle hooks — deferred past 0.4.0; no consumer needs them yet and
      the latency payload wants `queue_ms`/`handler_ms` designed together.
- [x] Public docs rewritten around declare / create / dispatch / work, plus
      `docs/onboarding/from-huey-celery.md`.

## Phase 5 — Safety checks (deferred past 0.4.0)

**Deferred by decision.** The first release ships the narrow Tier-1 policy surface —
execution, retry, failure classification — and nothing else. Safety metadata and
`dewey check` would add a second, larger policy vocabulary before the core one has
been used in production by anyone, which is the wrong order. `docs/concepts.md`
states plainly what Dewey does not guarantee (bounded batches, resumability,
idempotent side effects, timeouts) so the boundary is documented even though the
tooling is not built.

Revisit for 0.5 once a real consumer has run 0.4 in production.

- [~] (deferred) Add TaskPolicy safety metadata: `expected_runtime_s`,
      `stuck_threshold_s`, `batch_size`, `batch_size_required`, `resumable`,
      `idempotency_required`, `idempotency_key`, `concurrency_key`,
      `concurrency_limit`, `drain_mode`, and `safety_notes`.
- [~] (deferred) Add pure validator API:
      `validate_task_policy(policy, runtime_config) -> list[SafetyFinding]`.
- [~] (deferred) Add CLI: `dewey check` with human-readable output.
- [~] (deferred) Add CI-friendly output: `dewey check --format json`.
- [~] (deferred) Default behavior: errors exit non-zero; warnings exit non-zero only with
      `--strict`.
- [~] (deferred) Add `docs/task-safety.md` with safe/unsafe examples and the boundary
      between Dewey guarantees and handler responsibilities.
- [~] (deferred) Defer runtime safety scoring, source/AST linting, dashboard/admin UX, and
      richer partition/fairness primitives until after real usage.

## Phase 6 — Local integration infra and installed-wheel smoke

Keep the published package lean. Integration examples/tests should validate the
installed package without shipping a full example app inside the distribution.

- [x] Remove the repo-local FastAPI example to keep the published package lean.
- [x] `docker-compose.yml` with Postgres and Redis on offset ports (55440/56390).
- [x] Make targets: `up`, `down`, `test-integration`, `wheel-smoke`.
- [x] Local setup and env vars documented in README and CONTRIBUTING.
- [x] Wheel built in CI, with `twine check` and a contents listing.
- [x] Wheel installed into a fresh virtualenv, asserting the import resolves to
      site-packages rather than the source tree.
- [x] Smoke-imports for core and every advertised extra.
- [x] Installed-wheel end-to-end: migrate, create, dispatch, process, complete.
- [x] Huey/Redis smoke including duplicate delivery, broker outage and recovery,
      rolled-back producer, and retry-then-dead-letter.

## Phase 7 — Django production readiness

Django remains supported, but Django-specific operational polish does not block
the first public pre-release unless a Django consumer becomes the first
integration target.

- [x] Initial Django migration for the task model, shipped in the wheel, with a test
      that fails if models and migrations drift. One table: `task_entries`. The
      notification models were removed from this release rather than migrated.
- [x] `python manage.py migrate` verified on a fresh database, plus rollback and
      reapply, and the partial-index DDL inspected in real Postgres.
- [x] `dewey[django]` smoke-tested from the built wheel.
- [x] Added `manage.py dewey_dispatcher` — a consumer needs a supported way to run
      the dispatcher. Django admin remains deferred.

## Phase 8 — Changelog/version/release polish

- [x] Changelog section is `[0.4.0] - Unreleased`. `0.3.0` cannot be published:
      PyPI's `dewey` project already carries `0.3` and `0.3.0` from the original
      project, and PyPI never permits reusing a version string.
- [x] Dispatcher and TaskPolicy changes recorded there. Safety checks are deferred
      (Phase 5).
- [x] Date-stamp changelog at tag time.
- [x] Define 1.0 release gate in README/changelog notes: at least one real
      consumer must validate the public API before `1.0`.

## Phase 9 — Free OSS tooling/security

- [x] Coveralls coverage upload.
- [ ] GitHub CodeQL for Python.
- [x] Dependency audit. An OSV query over the locked set runs clean (Django, Huey,
      SQLAlchemy, asyncpg, psycopg2); CI now runs `pip-audit` over the exported
      lockfile as an advisory job, since pip-audit cannot build its temp venv on
      this dev machine.
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
- [x] Documented Postgres-first semantics: README opens on it ("losing the broker
      costs you latency, it cannot cost you work"), `docs/concepts.md` has
      "Postgres is the scheduler" and states that polling is the correctness path
      while LISTEN only shortens the wait, and `docs/adapters.md` describes what a
      broker outage looks like. Proven by the broker-outage legs of the
      installed-wheel smoke and the lab's `db-outage` scenario.
- [x] Documented the cohabiting setup in `docs/getting-started.md`: separate
      engine/session factory for Dewey, bounded pool, `max_overflow=0`,
      `pool_timeout`, statement/lock/idle-transaction timeouts, the extra LISTEN
      connection to budget for, and a separate DB role with a connection limit.
      Grounded in the lab's cohabitation scenarios rather than guesswork.
- [x] Documented the Django alias + router setup, including the `DEWEY["DATABASE"]`
      setting that already routes Dewey's own queries to it.

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
      workers can idle on. It round-trips through the same `task_entries`
      table so brokers stay optional.
- [x] Ship the helper behind the existing `dewey[async]` / `dewey[sqlalchemy]`
      extras; `dewey[async]` now includes asyncpg so LISTEN works without a
      new Dewey extra.
- [x] Keep the listener contract validated through library tests and future
      installed-wheel smoke tests.
- [~] Re-ran `priority-lane` and `priority-lane-batch-pressure` against the
      dispatcher architecture: both pass (3.06s and 16.38s drain). Baselines were
      *not* promoted — the run used scenario copies on a non-default port and
      `--skip-compare` — so re-baselining on the canonical scenarios is still to do.

## Lifecycle hooks — latency payload (future)

- [ ] When lifecycle hooks land, the `task_completed` hook payload must
      include `queue_ms`, `handler_ms`, `end_to_end_ms` (derivable from
      `created_at` / `started_at` / `completed_at`). This is the durable
      surface for runtime latency telemetry; integration tests can use window
      SQL until then.
- [ ] Add partial index on `task_entries(completed_at) WHERE completed_at IS
      NOT NULL` to keep windowed percentile queries cheap.
- [x] Fix every executor to stamp `completed_at` at actual completion time,
      not claim time; latency gates depend on this.

## Correlation-context extraction (future)

- [ ] When a second consumer wants the trace-context primitives, extract the
      ~50 LOC of generic helpers
      (ContextVar + filter + get/set/reset) into a tiny shared package such as
      `correlation-context`. Dewey then keeps only the metadata-round-trip glue.
- Defer until: 2nd consumer materialises.

## Phase 10 — First real consumer

- [x] Wired into a real async SQLAlchemy consumer (FastAPI + asyncpg) and exercised
      under chaos before release: Toxiproxy-injected Postgres latency, a mid-flight
      Postgres outage, worker kills, cohabitation pressure, and drain/p95 gates
      under load. 10/10 scenarios plus `worker-kill-check` pass
      (`notification-pressure` retired with the removed notification layer).
- [x] Broker stays transport, Postgres stays the guarantee — proven by the
      broker-outage leg of the installed-wheel smoke and the DB-outage scenario.
- [x] Integration rough edges fed back before release. Two of them were real
      defects the unit suite could not see: a dispatcher that exited on a database
      blip, and the absence of an async dispatcher at all.
- [ ] Cut over a production task type, one at a time, after publishing. Deliberately
      after: the point of publishing 0.x is to learn from real usage, and the
      pre-release validation above is what makes that safe to start.
- [ ] Tag/publish `0.4.0` once a human approves the release-candidate report.
- [ ] Promote to `1.0` only after real production usage proves the API stable.
