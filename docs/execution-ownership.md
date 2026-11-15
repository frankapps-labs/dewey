# Execution ownership

What it means for a worker to "have" a task, what that protects, and what it cannot.

## Leases and tokens

Claiming a task writes an **assignment**: `dispatch_token`, `worker_id`,
`lease_expires_at` and `execution_deadline_at`. Starting it locks the row, validates
that exact assignment and its unexpired lease, consumes one attempt and creates a
fresh `execution_token`. Completion and failure lock and reload the row and require
`PROCESSING`, a live lease and deadline, and the exact token captured at start. A
stale execution's report returns `False` without modifying status, error, retry
schedule, attempt count or timestamps.

- The hard deadline is measured from assignment. Renewal keeps a lease alive; it
  cannot resurrect an expired one or extend the deadline.
- Database time is sampled after locking, not at transaction start.
- Success and failure clear the token. Lease recovery, operator retry and kill
  invalidate it. Operator retry resets the attempt count; crash recovery keeps
  consumed attempts and honours the budget.
- `latest_outcome` is the most recent outcome's kind (`completed`, `handler_failure`,
  `interrupted`, `timed_out`, `abandoned`, `cancelled`, `expired`), not a history; the
  task's events are the history.

The tokens are internal. They are not task results, authorization secrets or
cancellation commands. Do not set them from task arguments or edit them by hand.

Use PostgreSQL **READ COMMITTED** isolation with dedicated sessions. Under
REPEATABLE READ or SERIALIZABLE, conflicting locked rows raise serialization errors;
never read those as successful completion.

## What is not fenced

- **External side effects.** An old handler may keep running after its row was
  recovered or killed. Killing a task row does not stop its OS process by itself; the
  worker stops task processes at their deadline and when their lease is lost.
  Handlers still need idempotency: use
  [stable effect keys and durable dedupe](idempotency.md).
- **Death of the worker's own supervisor.** The supervisor contains its worker group;
  if the supervisor is killed outright, only an enclosing container or cgroup
  guarantees no task process survives it.

No exactly-once guarantee follows from any of this. Delivery means a committed task
stays in the ledger until an explicit terminal outcome (completed, dead, expired or an
operator action); it does not mean every attempt succeeds or that recovery is
instantaneous.

## How the worker uses it

The worker parent reserves only free process slots and reconciles every slot against
the ledger before reusing it: a Python return value never frees capacity by itself.
If the parent is unsure whether a claim committed, it stops its local children and
lets the committed leases expire, so another worker recovers them. A missing result
from a task process never justifies deleting a task, marking it completed or blindly
repeating its effects.

Recovery distinguishes an assignment that never started (returned to `PENDING`, no
attempt used), an interrupted execution and a hard timeout (both retried per policy,
attempt counted). A task timeout spends the task's attempt, not the worker's
crash budget (`max_restarts`); real task-process crashes do spend it.

Django uses synchronous ledger storage; SQLAlchemy supports sync or async sessions.
Both run sync or awaited async handlers, one task per process. Async Django handlers
must use Django's own bridges for ORM calls. Handlers must await their background
work: leftover async tasks are cancelled before the process is reused.

Recovery uses `SKIP LOCKED`, so one blocked row cannot freeze a pass, but a hung
database transaction holding a row can postpone that row's recovery indefinitely.
`dewey doctor` reports leases that expired minutes ago and are still unrecovered.
Configure lock, statement and idle-in-transaction timeouts, and resolve the owning
transaction through normal database operations rather than editing ownership columns.

A worker recovers only the queues it serves. Keep at least one worker running for
every queue that has work.

## Startup check

Workers check the schema and isolation level before claiming anything and refuse to
start on a mismatch. To run the same check yourself:

```python
from sqlalchemy.orm import Session
from dewey.sqlalchemy import check_readiness

with Session(engine) as startup_session:
    check_readiness(startup_session)
```

(`check_readiness_async` for an `AsyncSession`; `manage.py dewey doctor` for Django.)
It raises `ReadinessError` for a mismatch, never migrates and never mutates tasks.

## Schema

Django ships every migration; run `migrate` on each database alias Dewey uses.
SQLAlchemy users generate a revision from `Base.metadata` (`create_all()` does not
alter existing tables). For a table created before these columns existed, the
additions are:

```sql
ALTER TABLE task_entries ADD COLUMN execution_token varchar(36) NULL;
ALTER TABLE task_entries ADD COLUMN dispatch_token varchar(36) NULL;
ALTER TABLE task_entries ADD COLUMN worker_id varchar(36) NULL;
ALTER TABLE task_entries ADD COLUMN lease_expires_at timestamptz NULL;
ALTER TABLE task_entries ADD COLUMN execution_deadline_at timestamptz NULL;
ALTER TABLE task_entries ADD COLUMN latest_outcome varchar(32) NULL;
CREATE INDEX ix_task_native_lease ON task_entries (lease_expires_at)
  WHERE lease_expires_at IS NOT NULL;
ALTER TABLE task_entries ADD COLUMN execution_timeout_s double precision NULL;
ALTER TABLE task_entries ADD CONSTRAINT ck_task_execution_timeout
  CHECK (execution_timeout_s > 0 AND execution_timeout_s < 'Infinity'::float8);
ALTER TABLE task_entries ADD COLUMN cancel_requested boolean NOT NULL DEFAULT false;
```

`execution_timeout_s` is an optional per-attempt budget stamped at creation. A claim
uses `now + min(row budget, task_timeout_seconds)`; the worker's value is a ceiling.
Retries keep the row's budget, and renewals never move the deadline.

No backfill is needed: existing rows keep null ownership. See
[upgrading to 0.6](upgrading-0.6.md) for the rest of the move from 0.5.
