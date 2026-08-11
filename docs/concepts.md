# Concepts

## Postgres is the scheduler

The usual arrangement has your application enqueue to a broker, and a ledger — if there
is one — record what happened afterwards. Dewey reverses the direction of authority:

- A task exists when its **row** exists. Nothing is enqueued anywhere first.
- A **dispatcher** decides what is ready and hands task IDs to a transport. Sync and
  async implementations exist and behave identically.
- The **transport** carries an ID to a worker. That is its entire job.
- A **worker** loads the row, runs the registered handler, and records the outcome.

The practical consequences: the backlog is queryable in SQL, priorities and scheduling
are `ORDER BY` rather than broker features, and losing the broker delays work instead of
destroying it.

## The state machine

```text
PENDING ──► DISPATCHING ──► PROCESSING ──► COMPLETED
   ▲             │               │
   │             │               ├──► FAILED ──► PENDING   (retry, after backoff)
   │             │               │         └───► DEAD      (attempts exhausted)
   └─────────────┴───────────────┘                          (timeout sweep)
```

| State | Meaning | How it leaves |
|---|---|---|
| `PENDING` | Ready, or waiting for `scheduled_for`. | A dispatcher claims it, or a worker takes it directly. |
| `DISPATCHING` | Claimed and handed to the transport; no worker has started it. | A worker starts it, dispatch failed and it returns to `PENDING`, or the timeout sweep reclaims it. |
| `PROCESSING` | A worker is running the handler. | Handler returns, raises, or the worker dies and the stuck sweep reclaims it. |
| `COMPLETED` | Terminal. | Nothing. |
| `FAILED` | Failed, retry scheduled at `scheduled_for`. | The sweep returns it to `PENDING`, or dead-letters it once attempts are spent. |
| `DEAD` | Terminal until a human intervenes. | Manual retry (`DEAD → PENDING`). |

`PENDING → PROCESSING` stays legal so that in-process execution — no broker in the path
— and manual retry both work. Terminal rows are never redispatched, and `FAILED` is only
ever redispatched by way of `PENDING`.

## Why `DISPATCHING` exists

Without it, a dispatcher that dies after handing an ID to the broker but before the
broker durably accepts it leaves a row that looks ready and a message that never
arrived. `DISPATCHING` plus `dispatching_at` makes that window visible, so the sweep can
reclaim it after `dispatch_timeout_seconds`.

The timeout must exceed the worst-case time a task can wait in your broker before a
worker starts it. Set it too low and work that is merely queued gets reclaimed and
dispatched twice.

## Claiming

The claim is what makes concurrent dispatchers safe:

```sql
SELECT id FROM task_entries
WHERE status = 'pending' AND (scheduled_for IS NULL OR scheduled_for <= now())
ORDER BY priority DESC, COALESCE(scheduled_for, created_at) ASC, created_at ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
-- then: UPDATE those IDs to 'dispatching', and commit
```

`SKIP LOCKED` means a dispatcher steps over rows another one is holding rather than
waiting for them, so N dispatchers cooperate without a leader election. The claim is
committed *before* the transport is touched, so a crash between the two leaves a
recoverable row rather than a lost one.

Ordering is: higher `priority` first, then earliest due, then oldest. `priority`
defaults to `0`; a higher number goes first.

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

Tier 1 fields: `queue`, `priority`, `max_attempts`, `backoff`, `retry_on`,
`fail_fast_on`. An unknown field is an error at declaration time, so `max_attemps=3`
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
- **An unknown task type is a failed attempt, not a crash.** A worker deployed before
  the handler retries with backoff and only dead-letters once the budget is spent —
  rather than destroying work it merely does not recognise yet.

## The sweep

The sweep is the recovery pass, and the dispatcher runs it on `sweep_interval_seconds`:

| Pass | Finds | Does |
|---|---|---|
| `sweep_failed` | `FAILED` rows whose `scheduled_for` has passed | Returns them to `PENDING`, or `DEAD` if attempts are spent |
| `sweep_dispatching` | `DISPATCHING` rows older than `dispatch_timeout_seconds` | Returns them to `PENDING` |
| `sweep_stuck` | `PROCESSING` rows older than `stuck_threshold_minutes` | Returns them to `PENDING` |

The dispatch-timeout sweep is the backstop for a dispatcher that *died*. A dispatcher
that is still running and merely could not write a release — because the database was
the thing that failed — remembers those IDs and retries the release before claiming more
work, so recovery there does not wait out `dispatch_timeout_seconds`.

**`stuck_threshold_minutes` must exceed your longest legitimate handler runtime.** The
stuck sweep cannot tell a slow handler from a dead worker. Set the threshold below a
real runtime and a handler that is still working is presumed abandoned: the row returns
to `PENDING` and the task runs a second time, concurrently with the first.

Because `FAILED → PENDING` is a sweep transition, **nothing retries unless something
runs the sweep.** The dispatcher owning that tick means one process to operate rather
than two; the functions stay public if you would rather drive them from cron.

## Wake-up

`create_task` issues a Postgres `NOTIFY` inside your transaction. Postgres holds
notifications until commit, which is the whole guarantee: a rolled-back producer never
wakes a dispatcher, and no `on_commit` bookkeeping is needed for that to hold.

The dispatcher listens where the driver supports it (psycopg2 and psycopg3 both do) and
**polls regardless**. Polling is the correctness path — it finds work whose notification
was missed and work that only became due because `scheduled_for` passed. LISTEN only
shortens the wait.

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
`NULL`s as distinct, so unkeyed tasks never collide with each other. Derive the key from
a domain command ID and a duplicate producer call becomes an `IntegrityError` you can
catch rather than a second delivery.

Handlers should still be idempotent. Duplicate *transport* delivery is already harmless
— the claim is atomic and a redelivered ID is a logged no-op — but a worker killed
between a side effect and its commit can genuinely run a handler twice.

## What Dewey does not do

- **It does not make your handler safe.** Bounded batches, resumability and idempotent
  side effects are the handler's job. Dewey can retry a killed task; it cannot know that
  item 17 of 25 was already committed.
- **It does not enforce timeouts.** A handler that hangs holds its worker until the
  stuck sweep notices. Use your worker's own timeout for a hard limit.
- **It is not a cron.** Trigger periodic work with cron, a Kubernetes CronJob, or your
  broker's periodic tasks, and have that trigger call `create_task`.
- **It is not built for a firehose.** Every task is a Postgres write. That is roughly
  1-2k tasks/sec before tuning, which is plenty for background work and the wrong tool
  for an event stream.
- **No fairness, rate limiting or concurrency caps yet.** Not in this release.
