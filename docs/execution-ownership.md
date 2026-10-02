# Execution ownership (unreleased)

The execution-ownership change is not part of the published 0.5.3 release. It
protects outcome writes in Django and SQLAlchemy sync/async executors, including
the existing Huey transport. It does not provide a native worker service.

## What is fenced

Every committed `PROCESSING` claim receives a fresh internal `execution_token`.
Completion and failure lock and reload the row, then require both `PROCESSING`
and the exact token captured by that execution. A stale worker returns `False`
without modifying status, error, retry schedule, attempt count or timestamps.

Success/failure clear the token. Stuck recovery, operator retry/bulk retry and
kill invalidate it. Explicit operator retry retains its existing attempt-reset
behavior; ordinary crash recovery retains consumed attempts and honors the budget.
SQLAlchemy locked reads refresh cached objects even with `expire_on_commit=False`.
Use a dedicated execution session, as before, with PostgreSQL **READ COMMITTED**
isolation. `populate_existing` refreshes the identity map, not a transaction's
snapshot. REPEATABLE READ/SERIALIZABLE can instead raise serialization errors on
conflicting locked rows; do not interpret those errors as successful completion.

The token is internal, not a public task-result field, authorization secret,
lease or cancellation command. Do not set it from task arguments or manually
edit it to control processing.

## What is not fenced

- External side effects: an old handler may continue after recovery or an operator
  action. Killing a task row does not stop its OS process. Handlers still need
  idempotency.
- ID-only transport messages and dispatch release operations: no dispatch token
  validation or generation-aware assignment protocol is introduced here.
- Lease renewal, native admission, parent-death containment and recovery timing:
  this change does not implement those mechanisms.

No exactly-once or mixed-version safety guarantee follows from this column.

## Delivery and recovery prerequisites

Delivery means a committed task stays in the ledger until an explicit terminal
outcome (including dead-letter/expiry/operator action); it does not mean every
attempt succeeds, external effects are exactly once, or recovery is instantaneous.
Dispatcher and recovery sweeps must keep running, with monitored heartbeats,
overdue work and dead-letter queues. Set task timeouts above legitimate handler
runtimes, and make handlers idempotent before enabling crash recovery.

The internal process pool is not connected to dispatch yet. Its RETURNED signal
is not a persisted task completion. STOPPED/LOST mean the durable result is unknown;
the future runner must reconcile the task's ownership and database outcome before
acknowledging the ticket or releasing capacity. A missing IPC result cannot justify
deleting a task, marking it completed or blindly repeating its external effects.

Recovery uses `SKIP LOCKED` so one blocked row cannot freeze the whole pass. A hung
but live database transaction can indefinitely postpone that row's recovery. When
stuck recovery selects nothing but an overdue row remains, it logs a warning;
this bounded probe is a diagnostic, not proof of lock starvation (rows can change
concurrently). Monitor overdue-task age even when other rows are being recovered,
and database lock/idle-in-transaction age. Configure application-appropriate lock,
statement and idle-in-transaction timeouts; investigate/cancel the owning transaction
through normal database operations rather than bypassing locks or ownership checks.

## Read-only startup preflight

Run Django `dewey_doctor` for every served alias; it also checks READ COMMITTED.
SQLAlchemy applications should explicitly run `check_readiness` before starting
any dispatcher, executor or recovery loop:

```python
from sqlalchemy.orm import Session
from dewey.sqlalchemy import check_readiness

with Session(engine) as startup_session:
    check_readiness(startup_session)
```

For async applications, use `async with AsyncSession(engine)` and
`await check_readiness_async(startup_session)`. Both check PostgreSQL, isolation,
required task columns and the nullable `varchar(36)` ownership column. They raise
`ReadinessError` for mismatches; connection/permission failures propagate. The
caller owns the inspection transaction (closing the dedicated session rolls it
back). These checks do not migrate, commit or mutate tasks and are not implicit
per-task migrations or continuous health checks. An executor running without this
preflight against an unmigrated table can still fail with a database schema error.

Ownership checks use primary-key row reads followed by token comparison; an
additional token index does not improve those accesses. Add one only if a real
future workload searches by token alone, not as a speculative delivery fix.

## Coordinated upgrade

1. Stop claims and drain **all** old executors, dispatchers/recovery jobs and
   operational writers for every affected queue/database. Resolve active legacy
   work before enabling upgraded writers. Old code checks only status and cannot
   honor execution tokens.
2. Apply the additive schema using the application's migration process. Django
   ships migration `0003_execution_token`; run it on every served database alias.
   SQLAlchemy users should generate/review an Alembic revision from the current
   metadata. `Base.metadata.create_all()` does not alter existing tables. The
   equivalent PostgreSQL addition is:

   ```sql
   ALTER TABLE task_entries ADD COLUMN execution_token varchar(36) NULL;
   ```

   No destructive backfill or attempt reset is required; existing rows keep a
   null token and their other data. Review an already-existing column rather than
   silently accepting an incompatible definition.
3. Start only upgraded writers. Abandoned legacy `PROCESSING` rows retain existing
   timeout recovery; the next execution claim receives a token. A null legacy
   token is not permission for a new executor to complete an unclaimed task.

For rollback, retain the additive column and prefer changing transport while
keeping upgraded fenced code. Downgrading to status-only writers loses protection;
removing the column is not an automatic rollback procedure. Schema/traffic changes
remain application/operator decisions, not worker startup side effects.
