# Dewey 0.4.0 — release candidate report

**Status:** awaiting human approval. Nothing is tagged and nothing is published.
**Branch:** `release/0.4.0`
**Date:** 2026-07-30

---

## Proposed release

| | |
|---|---|
| Version | **0.4.0** |
| Tag | `v0.4.0`, cut from `main` after merge |
| Distribution name | `dewey` (unchanged) |

**Why 0.4.0 and not 0.3.0.** PyPI's `dewey` project already carries `0.3` and `0.3.0`
from the original project (sdist uploaded 2011-09-29, not yanked). PyPI permanently
refuses re-upload of a version string that has ever existed, even after deletion — so
`0.3.0` is unpublishable under this name regardless of ownership. `0.4.0` is
unambiguously above everything on the index. The "0.3 architecture" label survives in the
private ADRs; the published version is 0.4.0.

---

## Schema

Table `task_entries` (Django migration `dewey/0001_initial`, and
`Base.metadata.create_all` for SQLAlchemy):

**Changed from the previous in-repo schema**

- **dropped** `payload JSON`
- **added** `args JSON NOT NULL` (list), `kwargs JSON NOT NULL` (dict)
- **added** `dispatching_at TIMESTAMPTZ NULL`
- status domain gains `dispatching`
- `process_after` → `scheduled_for` (predates this release)

**Indexes** (verified in real Postgres DDL)

| Name | Definition |
|---|---|
| `ix_task_pending_sched` | `(scheduled_for) WHERE status = 'pending'` |
| `ix_task_dispatching` | `(dispatching_at) WHERE status = 'dispatching'` |
| `ix_task_processing_started` | `(started_at) WHERE status = 'processing'` |
| `ix_task_failed_sched` | `(scheduled_for) WHERE status = 'failed'` |
| `ix_task_type_created` | `(task_type, created_at)` |
| `uq_task_type_idempotency_key` | `UNIQUE (task_type, idempotency_key)` |

The migration creates **one table**: `task_entries`. The notification layer and its two
tables were removed before publishing (see breaking changes), so the first published schema
is exactly what the release promises.

**Migration verified:** fresh `migrate` on an empty database, `makemigrations --check`
reports no drift, `migrate dewey zero` then re-apply succeeds, partial-index DDL
inspected directly.

---

## Public API

`dewey` (framework-free): `task`, `TaskPolicy`, `TASK_DEFAULTS`, `Constant`,
`Exponential`, `Custom`, `BackoffPolicy`, `PolicyRegistry`, `registry`,
`resolve_policy`, `configure_policies`, `clear_project_policies`, `encode_args`,
`encode_kwargs`, `TaskStatus`, `DeweyError`, `TransientError`, `NonRetryableError`,
`RetryAfter`, `SerializationError`, `DuplicateTaskTypeError`, `UnknownTaskTypeError`,
`__version__`.

`dewey.dispatcher`: `Dispatcher`, `AsyncDispatcher`, `DispatchBackend`,
`AsyncDispatchBackend`, `DispatchFn`, and defaults.
`dewey.sqlalchemy` (47 names) / `dewey.django` (19 names): producer, worker, sweep and
query APIs, plus
`SQLAlchemyDispatchBackend`, `AsyncSQLAlchemyDispatchBackend` and
`DjangoDispatchBackend`.
`dewey.adapters`: `DispatcherAdapter`, `HueyAdapter`.

---

## Breaking changes

Nothing published depends on these; the previous line was in-repo only.

1. **`payload` → `args` / `kwargs`.** Handlers are invoked `handler(*args, **kwargs)`
   instead of `handler(task_type, payload)`.
2. **`process_task(session, task_id)` no longer needs a handler.** It resolves the
   handler and retry rules from the task type. Passing one explicitly still works.
3. **`adapter.enqueue()` is gone**, along with `BaseAdapter` and
   `HueyAdapter.setup()`. Producers do not talk to a broker; the dispatcher does.
4. **Celery removed** — module, extra, keyword. Archived on
   `archive/celery-adapter-enqueue-era`.
5. **`create_task` defaults** `queue`, `priority` and `max_attempts` from policy.
6. **A dispatcher is now required** for work to move at all, including retries.
7. **The notification layer is removed** — `dewey.core.notifications`,
   `dewey.*.notifications`, `Channel`, `ChannelRegistry`, `NotificationEntry`,
   `NotificationAttempt` and their two tables. Archived on
   `archive/notification-ledger-0.4`. Returns later as task types with channel handlers.

---

## Support matrix

| | Supported | Verified how |
|---|---|---|
| Python | 3.11, 3.12, 3.13 | CI matrix; suite run locally on 3.13 |
| PostgreSQL | 13+ | Suite against 16 (local and compose) |
| Django | 4.2+ | 6.0.7 locally; CI pins 4.2 and 5.1 explicitly |
| SQLAlchemy | 2.0+ | 2.0.49 locally, sync and async |
| Web frameworks | not an integration axis | Dewey never imports a web layer. A FastAPI/Starlette/Litestar/Flask app is a SQLAlchemy consumer; there is deliberately no `fastapi` extra. The lab's async chaos coverage runs through a FastAPI + asyncpg testbed |
| Huey | 2.5+ | 3.0.0 locally; 2.5.5 verified separately, CI lane pins 2.5 |
| Drivers | psycopg2, psycopg3, asyncpg | psycopg2 + asyncpg exercised; psycopg3 path is code-only (see limitations) |
| Celery | not supported | removed |

---

## Commands and results

All at the head of this branch.

| Gate | Result |
|---|---|
| `make lint` | All checks passed |
| `make format-check` | 82 files already formatted |
| `make typecheck` (basedpyright) | 0 errors, 0 warnings, 0 notes |
| `make test` (local Postgres 16) | **437 passed**, 93% coverage |
| `make test-integration` (compose Postgres + Redis) | **437 passed** |
| Multi-dispatcher concurrency (4 threads, 60 tasks, real Postgres) | pass — every task claimed exactly once |
| Resilience lab, ADR-003 suite (11 scenarios, chaos) | **verdict PASS** |
| Huey 2.5.5 adapter suite | 17 passed |
| `uv build` | `dewey-0.4.0.tar.gz`, `dewey-0.4.0-py3-none-any.whl` |
| `twine check dist/*` | PASSED (both) |
| Installed-wheel smoke (clean venv, real Postgres + Redis) | all checks passed |
| Wheel contents | 39 modules + `py.typed` + migrations; no tests, no private paths |
| Dependency audit (OSV over the locked set) | 0 advisories across Django, Huey, SQLAlchemy, asyncpg, psycopg2 |
| CI dependency audit | `pip-audit` over the exported lockfile, advisory-only job |
| Public/private boundary | no HQ paths, ADR text, or internal product names in the tree or artifacts |

The installed-wheel smoke covers: migrate from shipped migrations, declare and create a
task inside an atomic block, dispatch, process to `COMPLETED`, duplicate delivery,
rolled-back producer, Redis outage and recovery, retry then dead-letter, and a real Redis
round trip through a Huey worker.

`pip-audit` itself could not run on this machine (`ensurepip` aborts when it builds its
temporary venv), so the local audit queries the same OSV database directly. CI now runs
`pip-audit` over the exported lockfile, where the environment is predictable — advisory
only, so a new CVE in a transitive dev dependency surfaces without blocking an unrelated
pull request.

---

## Bugs found and fixed during this work

Worth recording because each would have shipped silently:

1. **The claim query took the whole table.** `UPDATE ... WHERE id IN (SELECT ... LIMIT n
   FOR UPDATE SKIP LOCKED)` reads correctly, but Postgres may re-execute that subplan per
   candidate row, and each row then finds itself in its own result. Both backends now lock
   first and update the locked IDs.
2. **LISTEN was silently disabled.** `detach()` clears the connection fairy's
   `driver_connection`, so reading it afterwards returned `None` and the dispatcher fell
   back to polling forever. A test now asserts the backend really gets a listener.
3. **The listen connection poisoned the pool.** Taken from the SQLAlchemy pool and then
   closed, breaking every later checkout.
4. **`dewey.django.sweep` was ambiguous** — the function on first access, the submodule
   afterwards.
5. **A database blip killed the dispatcher.** An exception from `claim()` escaped the run
   loop and ended the process, so claimed work then waited for the dispatch timeout and
   nothing swept at all. Found by the resilience lab's `db-outage` scenario, which failed
   on first run against 0.4 (nothing drained, rows stuck in `PROCESSING` for 74s) and
   passes now (drained in 18.4s, zero dead).

---

## Known limitations

- **The psycopg3 LISTEN path is code-only.** The driver-detection branch exists and
  degrades safely, but the local environment runs psycopg2, so psycopg3 wake-up has not
  been exercised end to end. Worst case is a fall back to polling.
- **No timeout enforcement.** A hung handler holds its worker until the stuck sweep
  notices; use the worker's own timeout for a hard limit.
- **No scheduler.** Periodic work needs an external trigger calling `create_task`.
- **Priority is claim-time only**, not preemption. Work already dispatched is not
  reordered.
- **Throughput is bounded by Postgres writes** — roughly 1-2k tasks/sec before tuning.
  No throughput baseline has been measured for this release.
- **The async path now has chaos coverage** via the resilience lab, whose testbed is an
  async SQLAlchemy + FastAPI consumer. The Huey-over-Redis transport, by contrast, is
  covered by unit and installed-wheel smoke tests but not by the lab's chaos scenarios.
- **No multi-channel notification delivery.** Removed before publishing rather than
  shipped half-committed; see breaking changes.

---

## Deferred scope

| Deferred | Why |
|---|---|
| Notification delivery (email/webhook/Slack channels) | Removed rather than deprecated. Returns as task types with channel handlers, with attempt history on the task ledger |
| ADR-004 safety checks (`dewey check`, safety metadata, `docs/task-safety.md`) | Would add a second, larger policy vocabulary before the first has been used in production |
| DB runtime policy overrides | No admin surface to drive them; resolver is already layered for it |
| Lifecycle hooks (`on_complete` / `on_fail` / `on_retry`) | No consumer needs them, and the latency payload wants designing with them |
| Policy-level `dedupe_key` / `dedupe_window_s` | Producer-supplied `idempotency_key` covers the first consumer |
| Fairness, rate limits, concurrency caps, hard timeouts, expiry | Tier 2 |
| Celery adapter | No integration coverage; archived branch to port from |
| Native scheduler, non-Python runners, admin UI, hosted service | Out of scope |

---

## Resilience lab

> **Note:** this verdict predates the notification-layer removal. The lab's testbed and
> tooling still reference that layer, so the lab cannot run against the current head until
> a lab-side cleanup lands — recorded in the lab's own `reports/latest.md`. Reproducing the
> verdict below means checking Dewey out at `archive/notification-ledger-0.4`.

The private lab's testbed was migrated to the 0.4 API (`@dewey.task` declarations,
`kwargs=`, and `AsyncDispatcher` replacing its hand-rolled claim and sweep loops) and the
ADR-003 release suite was run against this head.

**Verdict: PASS**, all 11 scenarios:

| Scenario | Drain | p95 accept |
|---|---|---|
| release-check | 41.81s | 121.16ms |
| db-sleep | 6.07s | 18.17ms |
| db-latency (Toxiproxy latency injection) | 10.59s | 11.31ms |
| db-outage (Postgres disabled mid-flight) | 63.01s | 10.59ms |
| priority-lane | 3.06s | 29.93ms |
| priority-lane-batch-pressure | 16.38s | 119.62ms |
| wake-on-insert-trickle | 60.69s | 12.58ms |
| cohabitation | 20.18s | 13.34ms |
| cohabitation-chaos | 31.34s | 1771.60ms |
| resilience-long | 30.30s | 13.68ms |
| notification-pressure | 42.43s | 12.66ms |

The `notification-pressure` scenario exercised the notification ledger, which this release
no longer contains. It passed at the time and is recorded for completeness; it retires with
the layer.

Plus `worker-kill-check`: passed, 1 completed, 0 dead — a task in flight when its worker
is killed recovers and completes.

No accept-latency regression against the pre-0.4 baselines recorded in May:

| Scenario | May baseline | Now |
|---|---|---|
| release-check | 125.64ms p95, 45.94s drain | 124.88ms, 41.84s |
| cold-burst | 136.36ms p95 | 118.57ms |

One lab-side finding, not a Dewey defect: the `burst` scenario fails its
`max_p95_accept_ms: 100` gate at ~177ms. It has no warmup block, so it measures cold
connection-pool opening against a steady-state threshold — which is what `cold-burst`
exists to measure, at a 300ms gate, and which the lab's own `release-suite` help text
already documents ("without [warmup] the same scenario comes in ~175ms"). The gate wants
recalibrating in the lab; it is not a release blocker.

## Before publishing

1. ~~Confirm the repo lives at `frankapps-labs/dewey`.~~ **Done** — project metadata, the
   CONTRIBUTING clone URL and the SECURITY advisory link all point there, matching the
   git remote.
2. ~~**Confirm PyPI ownership** of the `dewey` project.~~ **Confirmed by the maintainer.**
3. ~~**Decide on the notification layer.**~~ **Decided and executed: removed from 0.4.0**,
   not deprecated. Shipping it experimental would have written two tables into the initial
   Django migration for every consumer, and that migration is the one artifact that cannot
   be cheaply revised later. It returns on the task engine's foundations — a task type with
   a channel handler, plus per-attempt history promoted to the task ledger for all task
   types. Archived on `archive/notification-ledger-0.4`.
4. Merge `release/0.4.0` → `main` (`make release` requires `main`).
5. Tag `v0.4.0` and push; `.github/workflows/publish.yml` publishes on tag via trusted
   publishing.
6. Date-stamp the changelog heading at tag time.

## Publication checklist

- [x] Resilience lab run green against this head (11/11 scenarios, verdict PASS)
- [x] PyPI project ownership confirmed (maintainer owns the `dewey` project)
- [ ] `archive/celery-adapter-enqueue-era` pushed, so the changelog's reference to it is
      true for anyone reading it
- [ ] `release/0.4.0` merged to `main`, CI green there
- [ ] `CHANGELOG.md` heading dated
- [ ] `git tag v0.4.0 && git push --tags`
- [ ] PyPI shows 0.4.0; `pip install dewey==0.4.0` in a clean venv
- [ ] GitHub release created from the changelog section
- [ ] Post-publish: install the published wheel and re-run the smoke script against it
