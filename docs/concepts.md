# Concepts

## State Machine

Tasks move through five states:

| State | Meaning | Next states |
|-------|---------|-------------|
| **PENDING** | Ready to process | → PROCESSING |
| **PROCESSING** | Worker picked it up (SELECT FOR UPDATE) | → COMPLETED, FAILED, DEAD |
| **COMPLETED** | Done ✓ | Terminal |
| **FAILED** | Handler raised, will retry after backoff | → PENDING (sweep), DEAD |
| **DEAD** | Max attempts reached, needs human decision | Terminal |

**Sweep behavior:**
- Failed tasks past their `process_after` time → reset to PENDING, or DEAD if attempts are exhausted
- Tasks stuck in PROCESSING for >10 minutes (configurable) → reset to PENDING, or DEAD if attempts are exhausted

### Notification States

Notifications have their own state machine:

| State | Meaning | Next states |
|-------|---------|-------------|
| **PENDING** | Ready to send | → SENDING |
| **SENDING** | Channel delivery in progress | → SENT, FAILED, DEAD |
| **SENT** | Delivered ✓ | Terminal |
| **FAILED** | Delivery failed, will retry after backoff | → PENDING (sweep), DEAD |
| **DEAD** | Max attempts reached | Terminal |

## Exponential Backoff

Failed tasks get exponential backoff before retry. Defaults:

| Attempt | Tasks | Notifications |
|---------|-------|---------------|
| 1 | 2 min | 1 min |
| 2 | 4 min | 2 min |
| 3 | 8 min | 4 min |
| 4 | 16 min | 8 min |
| 5 | 32 min | 16 min |
| 6+ | 1 hour (cap) | 30 min (cap) |

Both jittered ±25% to prevent thundering-herd retries.

### Custom backoff per processor

Pass a `backoff` function to `process_task` / `process_task_async` /
`send_notification(_async)` / `process_notification(_async)` to override
for a specific worker, queue, or test:

```python
from datetime import timedelta

# Fast-retry queue for transient API errors.
fast = lambda attempts: timedelta(seconds=min(2 * (2 ** attempts), 30))
await process_task_async(session, task_id, handler, backoff=fast)

# Deterministic backoff for tests.
await process_task_async(session, task_id, handler, backoff=lambda _: timedelta(seconds=1))
```

The signature is `Callable[[int], timedelta]` (exported as
`dewey.core.backoff.BackoffFn`). Defaults are also exported as
`default_task_backoff` and `default_notification_backoff` if you want to
wrap them.

## Project Structure

The library is split into layers with clear dependency boundaries:

- **`core/`** — Pure Python. State machine, backoff, types, notification protocol. Zero dependencies.
- **`sqlalchemy/`** — Task + notification models, executor, sweep, queries. Sync + async. Requires `sqlalchemy>=2.0`.
- **`django/`** — Task + notification models, executor, sweep, queries. Requires `Django>=4.2`.
- **`adapters/`** — Huey, Celery. Requires the respective queue library.

The core module has **zero dependencies**. The SQLAlchemy, Django, and adapter layers are opt-in via extras.

## Why Not Just `asyncio.create_task()`?

`asyncio.create_task()` is RAM. Dewey is Postgres.

```python
# This lives in process memory:
asyncio.create_task(run_scan(url))
#   → process restarts → scan vanishes
#   → no retry, no dead letter, no audit trail
#   → nobody knows it happened

# This lives in Postgres:
await create_task_async(session, task_type="scan", payload={"url": url})
#   → row in Postgres — survives restarts, deploys, crashes
#   → process dies → sweep catches it in ≤5 min
#   → fails → exponential backoff → retry → dead letter
#   → SELECT * FROM task_entries WHERE status = 'dead'
```

The async layer isn't about making dewey work with async code. It's about **replacing** `asyncio.create_task()` with something that doesn't disappear when the process restarts.

Your scan pipeline stays async — it's still Playwright and httpx. The difference is that the *guarantee of execution* moves from "hope this coroutine finishes" to "Postgres knows about it and will keep trying until it works or a human decides to stop."

**The broker (Huey/Celery/Redis) is the fast path. Postgres is the guarantee.** If the broker is down, the task is still in Postgres. Sweep picks it up and re-enqueues when the broker comes back. No fallback path needed.

## Why Not Just Use Celery/Huey?

Celery and Huey are **transports**. If the broker loses a message, you'll never know. Dewey adds an **accountability layer**:

- Every task is written to Postgres before enqueue
- Sweep catches anything the broker dropped
- Dead letter stops infinite retries
- Full audit trail: `SELECT * FROM task_entries WHERE task_type = 'order.confirmed'`
- Dashboard-ready query API

The broker becomes interchangeable. Redis, RabbitMQ, whatever — the guarantees come from Postgres.
