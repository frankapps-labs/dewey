# Getting started

## Quick Start — SQLAlchemy

### 1. Create the table

```python
from dewey.sqlalchemy.models import Base

# With Alembic or direct:
Base.metadata.create_all(engine)
```

### 2. Write a task to the ledger

```python
from dewey.sqlalchemy.executor import create_task

with Session(engine) as session:
    task = create_task(
        session,
        task_type="order.confirmed",
        payload={"order_id": "ORD-123", "total": "£49.99"},
        queue="default",
        priority=60,
    )
    session.commit()

    # Enqueue to your broker
    adapter.enqueue(task.id, queue=task.queue, priority=task.priority)
```

### 3. Process tasks in your worker

```python
from dewey.sqlalchemy.executor import process_task

def my_handler(task_type: str, payload: dict):
    """Your business logic. Raise to fail, return to complete."""
    if task_type == "order.confirmed":
        send_confirmation_email(payload["order_id"])

def worker_process(task_id: str):
    with Session(engine) as session:
        process_task(session, task_id, handler=my_handler)
        session.commit()
```

### 4. Set up the sweep

```python
from dewey.sqlalchemy.sweep import sweep

# Call every ~5 minutes from a periodic task
with Session(engine) as session:
    result = sweep(session)
    session.commit()
    # Re-enqueue swept tasks
    for task_id in result["failed"] + result["stuck"]:
        adapter.enqueue(task_id)
```

### 5. Query the ledger

```python
from dewey.sqlalchemy.queries import (
    get_stats, get_failed, retry_task, kill_task, purge_completed,
)

with Session(engine) as session:
    stats = get_stats(session)
    # {"pending": 12, "processing": 3, "completed": 4891, "failed": 2, "dead": 1}

    failed = get_failed(session, task_type="order.confirmed")
    retry_task(session, task_id="abc-123")
    kill_task(session, task_id="def-456")
    purge_completed(session, older_than_days=30)
    session.commit()
```

## Quick Start — Async (FastAPI)

If your app is async (FastAPI, Starlette, etc.), use the `_async` variants. Same guarantees, same state machine — just `await`.

### Install

```bash
pip install "dewey[sqlalchemy,async]"
```

### 1. Write a task to the ledger

```python
from dewey.sqlalchemy.async_executor import create_task_async

async with async_session() as session:
    task = await create_task_async(
        session,
        task_type="scan",
        payload={"url": "https://example.com", "max_pages": 10},
    )
    await session.commit()

    # Enqueue to broker (Huey/Celery — still sync, that's fine)
    adapter.enqueue(task.id)
```

### 2. Process tasks — handler is async

```python
from dewey.sqlalchemy.async_executor import process_task_async

async def handle_scan(task_type: str, payload: dict):
    """Your async business logic. Raise to fail, return to complete."""
    await run_playwright_scan(payload["url"], payload["max_pages"])

async def worker_process(task_id: str):
    async with async_session() as session:
        await process_task_async(session, task_id, handler=handle_scan)
```

### 3. Sweep

```python
from dewey.sqlalchemy.async_sweep import sweep_async

async with async_session() as session:
    result = await sweep_async(session)
    await session.commit()
    for task_id in result["failed"] + result["stuck"]:
        adapter.enqueue(task_id)
```

### 4. Query

```python
from dewey.sqlalchemy.async_queries import get_stats_async, get_failed_async, retry_task_async

async with async_session() as session:
    stats = await get_stats_async(session)
    failed = await get_failed_async(session, task_type="scan")
    await retry_task_async(session, task_id="abc-123")
    await session.commit()
```

### Async notifications work too

Every notification function has an `_async` variant:

```python
from dewey.sqlalchemy.async_notifications import (
    create_notifications_for_event_async,
    send_notification_async,
    sweep_notifications_async,
)
```

### Why the handler is async

The whole point: your scan pipeline / Playwright / httpx calls are already async.
With sync dewey, you'd bridge back via `asyncio.run()` inside a Huey worker.
With async dewey, the handler is a coroutine — your async code plugs straight in.

```python
# Sync dewey in a Huey worker — needs a bridge:
@huey.task()
def process(task_id):
    asyncio.run(process_task_async(session, task_id, handler))  # awkward

# Async dewey in-process — no bridge:
asyncio.create_task(process_task_async(session, task_id, handler))  # native
```

## Quick Start — Django

### 1. Add to INSTALLED_APPS and migrate

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "dewey.django",
]
```

```bash
python manage.py migrate
```

### 2. Write a task to the ledger

```python
from dewey.django.executor import create_task

task = create_task(
    task_type="order.confirmed",
    payload={"order_id": "ORD-123", "total": "£49.99"},
    queue="default",
    priority=60,
)

# Enqueue to your broker
adapter.enqueue(task.id, queue=task.queue, priority=task.priority)
```

Note: the Django API doesn't take a `session` parameter — Django manages its own database connections.

### 3. Process tasks in your worker

```python
from dewey.django.executor import process_task

def my_handler(task_type: str, payload: dict):
    if task_type == "order.confirmed":
        send_confirmation_email(payload["order_id"])

def worker_process(task_id: str):
    process_task(task_id, handler=my_handler)
```

`process_task` uses two short `transaction.atomic` sections: one to claim the row with `SELECT FOR UPDATE` and commit `PENDING → PROCESSING`, then another to finalize `PROCESSING → COMPLETED/FAILED/DEAD` after the handler returns. The handler itself does not run while holding the row lock.

### 4. Set up the sweep

```python
from dewey.django.sweep import sweep

# Call every ~5 minutes from a periodic task
result = sweep()
for task_id in result["failed"] + result["stuck"]:
    adapter.enqueue(task_id)
```

### 5. Query the ledger

```python
from dewey.django.queries import (
    get_stats, get_failed, retry_task, kill_task, purge_completed,
)

stats = get_stats()
failed = get_failed(task_type="order.confirmed")
retry_task(task_id="abc-123")
kill_task(task_id="def-456")
purge_completed(older_than_days=30)
```
