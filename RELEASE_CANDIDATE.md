# Dewey 0.5.0 — release candidate report

**Status:** BLOCKED at the mandatory independent critical-review gate. Implementation,
local review/repair, installed-wheel proof, and release gates are complete; the bounded
Claude reviewer was unavailable because its subscription session limit was exhausted.
**Branch:** `feat/dewey-0.5-first-integration`
**Base:** `847b6c428dfea01240b1d8f93adbed30d86fc191` (`origin/main` at start)
**Release boundary:** no merge, tag, TestPyPI/PyPI upload, or downstream integration.

## Delivered scope

1. Django dispatch paths resolve module-level callables; object traversal examples are
   rejected and corrected.
2. Producer, dispatcher, and worker database roles are separate. Producer atomicity is
   explicitly same-alias/same-connection; split aliases are tested as non-atomic.
3. Django/Huey contrib wiring uses `djhuey.HUEY`, one retry-disabled registration, and
   `close_db`.
4. Due failed retries are directly claimable and wake pacing follows earliest due work,
   independent of recovery sweep latency.
5. `dewey.contrib.django_huey` exports importable `adapter`/`dispatch` and supports
   `WORKER_DATABASE` without making Django/Huey core dependencies.
6. Absolute `expires_at`, terminal `EXPIRED`, audit timestamps, migrations, queries, and
   pre-dispatch/pre-handler enforcement ship across supported ORM paths.
7. Savepoint-backed `create_or_get_task()` parity returns identical existing tasks and
   raises redacted `IdempotencyConflictError` for conflicting key reuse.
8. Multi-dispatcher database heartbeats, Django system checks, and human/JSON
   `dewey_doctor` readiness are implemented.

## Schema and API changes

Django migration `0002_task_expiry_and_dispatcher_heartbeat` adds nullable
`expires_at`, `expired_at`, and internal `initial_scheduled_for`, updates status choices,
adds the expiry partial index, and creates `dewey_dispatcher_heartbeats`. SQLAlchemy
metadata mirrors it; existing SQLAlchemy databases require the documented additive DDL or
an Alembic revision.

New public surfaces include `EXPIRED`, `IdempotencyConflictError`,
`create_or_get_task()` / async parity, `get_expired()`, `sweep_expired()`, heartbeat query
helpers/models, `dewey.contrib.django_huey`, `WORKER_DATABASE`, and `dewey_doctor`.
`create_task()` remains compatible. Metadata does not participate in idempotency equality;
all persisted immutable execution inputs do. Active `PROCESSING` handlers are never expired
out from under execution.

## Gate evidence

| Gate | Current evidence |
|---|---|
| lint / format / typecheck | pass |
| full local PostgreSQL suite | 597 passed, 93% coverage |
| compose-equivalent PostgreSQL/Redis suite | 596 passed at implementation head using existing Dewey compose services; worktree `make up` itself hit the already-allocated standard ports |
| Django 4.2.24 lane | 172 focused Django/contrib/readiness/idempotency tests passed |
| current Django lane | full suite passed on Django 6.0.7 |
| Huey 2.5.5 lane | 28 adapter/contrib tests passed |
| current Huey lane | full suite passed on Huey 3.0.0 |
| migration proof | fresh installed-wheel migrate; drift, 0001→0002, rollback/reapply, and real-index tests passed |
| build metadata | `uv build` and `twine check dist/*` passed for 0.5.0 |
| wheel audit | 51 entries; contrib, migration 0002, doctor, and `py.typed` present; tests/private paths/caches absent |
| optional imports | isolated core/SQLAlchemy/async/Django/Huey/contrib matrix passed |
| installed wheel | real PostgreSQL/Redis smoke passed: contrib dispatch, producer rollback, broker recovery, direct retry, expiry, idempotency conflict, doctor heartbeat readiness, real Huey queue |

Evidence/code head before this report-only status update:
`3142413fe226f3a2bfedbb436a96b6de968ec0d7`.

## Critical review

A fresh independent Opus reviewer was requested on the integrated exact head and failed to
start because the Claude subscription session limit was exhausted. No model transcript is
committed. The main OpenAI orchestrator completed a bounded critical audit and one repair
pass, fixing:

- a zero-second earliest-due wait that could hot-loop while another dispatcher held the
  visible row under `SKIP LOCKED`;
- doctor incorrectly accepting a queue-scoped heartbeat as readiness for all queues;
- system checks omitting the router-selected producer backend;
- unbounded stale-heartbeat cleanup work per beat;
- missing real-PostgreSQL concurrency proofs for due-retry and expiry claim races; and
- installed-wheel retry proof that previously forced the due timestamp instead of measuring
  policy timing independently from a disabled/long recovery sweep.

Focused verification and the full suite pass after those repairs. The frozen task packet
still requires independent cross-vendor review and one focused fix verification, so this
report deliberately does **not** claim `RELEASE_CANDIDATE_READY_FOR_OPERATOR`.

## Compatibility and limitations

- Status consumers must accept `expired`; expired work is not manually retryable.
- Custom dispatch backends should add `next_due()` and `heartbeat()`; dispatcher fallback
  remains safe during a 0.x migration.
- Doctor can inspect the current process registry and durable dispatcher heartbeats. Without
  an expected-task manifest it cannot prove handler completeness or inspect another
  worker's in-memory registry.
- The psycopg3 LISTEN path remains code-only locally; polling stays the correctness path.
- Dewey still does not enforce hard handler timeouts, provide cron scheduling, or make
  handler side effects idempotent.

## Gates remaining

1. When reviewer capacity resets, run the mandatory fresh independent critical review on the
   then-current exact head; repair verified findings once and run focused/full verification.
2. Push/open or update the PR, then human-review and merge it; rerun on the merge SHA if it
   differs.
3. Approve the changelog date and create `v0.5.0`.
4. Verify the trusted publish workflow and PyPI artifacts.
5. Ask the first integrator to rerun their installed-wheel integration suite against 0.5.0.
