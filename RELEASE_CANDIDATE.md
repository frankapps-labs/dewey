# Dewey 0.5.0 — release candidate report

**Status:** implementation complete; integrated critical review and final exact-head gate
capture pending.
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

## Evidence captured before critical review

| Gate | Current evidence |
|---|---|
| lint / format / typecheck | pass |
| full local PostgreSQL suite | 592 passed, 93% coverage |
| compose-equivalent PostgreSQL/Redis suite | 592 passed using existing Dewey compose services; worktree `make up` itself hit the already-allocated standard ports |
| Django 4.2.24 lane | 172 focused Django/contrib/readiness/idempotency tests passed |
| current Django lane | full suite passed on Django 6.0.7 |
| Huey 2.5.5 lane | 28 adapter/contrib tests passed |
| current Huey lane | full suite passed on Huey 3.0.0 |
| migration proof | fresh installed-wheel migrate; drift, 0001→0002, rollback/reapply, and real-index tests passed |
| build metadata | `uv build` and `twine check dist/*` passed for 0.5.0 |
| wheel audit | 51 entries; contrib, migration 0002, doctor, and `py.typed` present; tests/private paths/caches absent |
| optional imports | isolated core/SQLAlchemy/async/Django/Huey/contrib matrix passed |
| installed wheel | real PostgreSQL/Redis smoke passed: contrib dispatch, producer rollback, broker recovery, direct retry, expiry, idempotency conflict, doctor heartbeat readiness, real Huey queue |

These are pre-review results, not the terminal exact-head gate table. The final section is
updated only after the integrated review/fix pass and final committed-SHA verification.

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

## Operator gates remaining

1. Complete critical cross-vendor review, one repair pass, and focused verification.
2. Run all mandatory gates on the final committed SHA and replace this provisional table.
3. Review and merge the PR; rerun on the merge SHA if it differs.
4. Approve the changelog date and create `v0.5.0`.
5. Verify the trusted publish workflow and PyPI artifacts.
6. Ask the first integrator to rerun their installed-wheel integration suite against 0.5.0.
