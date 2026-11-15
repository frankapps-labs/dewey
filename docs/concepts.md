# Concepts

## Postgres is the ledger, the queue and the clock

The usual arrangement has your application enqueue to a broker, a scheduler decide
when periodic work is due, and a ledger — if there is one — record what happened
afterwards. Dewey puts all three in the database:

- A task exists when its **row** exists. Nothing is enqueued anywhere else.
- A **worker** claims ready rows under a lease, runs the registered handler in its own
  process and records the outcome. Any number of workers cooperate without a leader.
- A **schedule** is a row with a cursor. Any worker turns a due tick into a task row,
  in one transaction, so a tick happens exactly once.

The practical consequences: the backlog is queryable in SQL, priorities and delays are
`ORDER BY` rather than broker features, and there is no second system whose outage can
lose work or a tick.

## The state machine

```text
PENDING ──► DISPATCHING ──► PROCESSING ──► COMPLETED
   ▲             │               │
   │             │               ├──► FAILED ──► DISPATCHING (retry when due)
   │             │               │         └───► DEAD        (budget spent)
   └─────────────┴───────────────┘                            (lease expired)
     └──────────────► EXPIRED ◄──────────────┘                (before start)
```

| State | Meaning | How it leaves |
|---|---|---|
| `PENDING` | Ready, or waiting for `scheduled_for`. | A worker claims it. |
| `DISPATCHING` | Assigned to a worker under a lease; no task process has started it. | The task process starts it, or the lease expires and it returns to `PENDING` without using an attempt. |
| `PROCESSING` | A task process is running the handler, under a renewed lease and a hard deadline. | The handler returns or raises, or the lease or deadline passes and it is recovered. |
| `COMPLETED` | Terminal. | Nothing. |
| `FAILED` | Failed, retry scheduled at `scheduled_for`. | A worker claims it when due, or it becomes `DEAD` once attempts are spent. |
| `DEAD` | Terminal until a human intervenes. | Manual retry (`DEAD → PENDING`). |
| `EXPIRED` | Its absolute start deadline passed before the handler was invoked. | Nothing; create a new task if the work is still wanted. |

Terminal rows are never claimed again. `now == expires_at` is expired; expiry consumes
no handler attempt. Once a row is `PROCESSING`, Dewey does not expire it out from
under a running handler.

## Leases and recovery

A claim is not a flag, it is a **lease**: the row records which worker instance holds
it and until when. The worker parent renews the leases of its running tasks every few
seconds. If the worker dies, its leases run out, and the next worker to look recovers
those rows:

- A row that was assigned but never started returns to `PENDING`. No attempt is used.
- A row whose handler was running becomes `FAILED` (retry per policy) or `DEAD` if that
  was its last attempt. The interrupted attempt counts: the handler may have had
  effects.
- A row that ran past its hard deadline is treated the same way, after its process is
  stopped.

Each execution also carries a fresh token, required to record its outcome. A handler
that is still running after its row was recovered cannot overwrite the newer
execution's result. It can still have side effects, which is why handlers must be
[idempotent](idempotency.md). See [execution ownership](execution-ownership.md) for
exactly what is and is not fenced.

## Claiming

The claim is what makes concurrent workers safe:

```sql
SELECT id FROM task_entries
WHERE queue = ANY(:queues) AND task_type = ANY(:registered_types)
  AND (status = 'pending' OR (status = 'failed' AND attempts < max_attempts))
  AND (scheduled_for IS NULL OR scheduled_for <= now())
ORDER BY priority DESC, COALESCE(scheduled_for, created_at) ASC, created_at ASC
LIMIT :free_slots
FOR UPDATE SKIP LOCKED;
-- then: write the lease on those rows, and commit
```

`SKIP LOCKED` means a worker steps over rows another one is holding rather than
waiting for them, so N workers cooperate without a leader election. The lease is
committed *before* the handler runs, so a crash leaves a recoverable row rather than a
lost one.

Ordering is: higher `priority` first, then earliest due, then oldest. `priority`
defaults to `0`; a higher number goes first.

A worker claims only **task types it has handlers for**. During a rolling deploy, a
task whose handler exists only in the new code waits for a new worker instead of
failing on an old one.

> Folding this into a single `UPDATE ... WHERE id IN (SELECT ... LIMIT n FOR UPDATE SKIP
> LOCKED)` looks tidier and is wrong. Postgres may re-execute that subplan once per
> candidate row, and each row then finds itself in its own result — the claim quietly
> takes the whole table instead of one batch.

## Policy

A task type's behaviour is a `TaskPolicy`, resolved by merging three layers — lowest
precedence first:

```text
TASK_DEFAULTS  <  @dewey.task(...)  <  configure_policies(...)
```

- `TASK_DEFAULTS` — Dewey's baseline.
- `@dewey.task(...)` — where the person writing the handler leaves their hints.
- `configure_policies({...})` — the project-wide layer, which wins, so operational
  policy can be owned and reviewed in one place instead of scattered across handler
  modules.

Fields: `queue`, `priority`, `max_attempts`, `backoff`, `retry_on`, `fail_fast_on`,
and the database budgets `statement_timeout_s` and `lock_timeout_s`. An unknown field is an error at declaration time, so `max_attemps=3`
fails at import rather than silently doing nothing in production.

The `handler` is never overridable by a later layer. Runtime database overrides are a
deliberate future boundary: the resolver already takes layers in precedence order.

### Where the attempt budget lives

`max_attempts` is stamped onto the row at creation, from the resolved policy. In-flight
work therefore keeps the budget it was created with, even if policy changes under it.
Backoff, by contrast, is read at failure time — so a timing change takes effect
immediately, while a budget change applies to new work.

## Failure classification

Handlers do not retry themselves. They raise, and policy decides:

| Raise | Result |
|---|---|
| `TransientError` (or anything, by default) | Retry per backoff until `max_attempts`. |
| `NonRetryableError`, or anything outside `retry_on` | Dead-letter immediately, leaving the remaining budget unspent. |
| `RetryAfter(n)` | Retry at `max(n, policy_backoff)`. |

`RetryAfter` exists for providers that tell you when to come back (`Retry-After`
headers, rate-limit responses). The floor is the safety: a handler can ask for *later*
than the policy would, never earlier, so a misbehaving external API cannot pull retries
inside your own rate budget.

Two default choices worth knowing:

- **Anything retries by default.** An empty `retry_on` means the attempt budget, not the
  exception type, is what eventually stops a task.
- **An unknown task type is not an error.** A worker without a handler for a type
  never claims it; the row waits for a worker that has one, and `dewey doctor` reports
  it if none does.

## Schedules

A schedule is a declared trigger (`dewey.cron`, `dewey.every`) plus a stored cursor:
the last tick accounted for. Creating ticks is one transaction: lock the schedule row
(skipping it if another worker holds it), insert a task row for each due tick under
the key `schedule:<name>:<tick>`, advance the cursor. However many workers run, a tick
is one row. What happens to ticks that came due while nothing ran is each schedule's
declared choice. See [schedules](schedules.md).

## Arguments

Task arguments are stored as `args` (list) and `kwargs` (dict) and splatted into the
handler as `handler(*args, **kwargs)`. A row records a function call, not a bag of data,
so handlers stay ordinary Python functions and a wrong argument list fails with a plain
`TypeError` before any side effect runs.

Four rich types are converted, losing their Python type on the way in:

| Python | Stored as | Handler receives |
|---|---|---|
| `datetime` / `date` / `time` | ISO 8601 string | `str` |
| `UUID` | canonical string | `str` |
| `Decimal` | decimal string (exact digits) | `str` |
| `Enum` | the member's value | that value |

Rows stay readable in psql and the encoding stays language-neutral. `bytes`, ORM
instances, sets, non-string mapping keys, NaN and deep nesting are refused with a
message naming the offending position — pass an ID and let the handler load its own row.

## Idempotency

An `idempotency_key` is unique per `(task_type, idempotency_key)`. Postgres treats
`NULL`s as distinct, so unkeyed tasks never collide. `create_task()` keeps its compatible
`IntegrityError` behaviour. `create_or_get_task()` requires a key and returns the existing
row only when the post-serialization/default-resolution creation contract matches: task
type, args, kwargs, queue, priority, max attempts, original schedule, and expiry. Metadata
is excluded because it is operational context, not execution definition. Conflicting reuse
raises `IdempotencyConflictError` naming differing fields without payload values. A
savepoint preserves the caller's surrounding business transaction when concurrent creators
race.

Handlers should still be idempotent. A worker killed between a side effect and its
commit means the handler genuinely runs twice.

## What Dewey does not do

- **It does not make your handler safe.** Bounded batches, resumability and idempotent
  side effects are the handler's job. Dewey can retry a killed task; it cannot know that
  item 17 of 25 was already committed.
- **It does not deliver exactly once.** Delivery is at-least-once, with fencing so a
  stale execution cannot overwrite a newer outcome.
- **It is not a sandbox.** The worker isolates tasks in processes and stops them at
  their deadline; it relies on the enclosing container to contain everything.
- **It is not built for a firehose.** Every task is a Postgres write. That is roughly
  1-2k tasks/sec before tuning, which is plenty for background work and the wrong tool
  for an event stream.
- **It does not wake on notify.** An idle worker polls about once a second; that is the
  latency floor for a new task.
- **No workflows, fairness, rate limiting or concurrency caps.** Not in this release.
