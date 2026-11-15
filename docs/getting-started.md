# Getting started

Dewey has three parts, and they are the same on every stack:

1. **Producers** create task rows inside their own transactions.
2. **Workers** (`dewey run`) claim rows, run handlers and keep the clock for schedules.
3. **Operators** (`dewey ...`) look at queues, dead letters and schedules.

Pick your ORM below. Everything else in the documentation applies to all of them.

## Before you start

- PostgreSQL 13 or newer, with the default `READ COMMITTED` isolation.
- Python 3.11 or newer.
- Workers run in a container (or another kill-all boundary); see
  [the worker](native-worker.md).

---

## Django

### 1. Install and migrate

```bash
pip install "dewey[django]"
```

```python
# settings.py
INSTALLED_APPS = [
    ...,
    "dewey.django",
]
```

```bash
python manage.py migrate
```

### 2. Declare tasks

Put handlers in a `tasks.py` in any installed app: the worker imports every app's
`tasks` module.

```python
# myapp/tasks.py
import dewey
from myapp.models import Order

@dewey.task("orders.confirm", max_attempts=5)
def confirm_order(order_id: str) -> None:
    order = Order.objects.get(id=order_id)
    if order.confirmation_sent_at:
        return                      # idempotent: a retry finds the work already done
    send_confirmation(order)
    order.mark_confirmation_sent()
```

### 3. Create work

```python
from django.db import transaction
from dewey.django import create_task

with transaction.atomic():
    order = Order.objects.create(...)
    create_task(task_type="orders.confirm", args=[str(order.id)])
```

The task row is written on the same connection as the order, so they commit or roll
back together. A task can also be delayed (`scheduled_for=`), given a deadline
(`expires_at=`), a priority, a queue, a per-task time limit (`execution_timeout_s=`) or
an idempotency key (`create_or_get_task`).

### 4. Schedule what runs on a clock

```python
# myapp/tasks.py
@dewey.task("orders.expire_carts", schedule=dewey.cron("*/15 * * * *"))
def expire_carts() -> None:
    ...
```

```bash
python manage.py dewey schedules plan     # see what would change
python manage.py dewey schedules apply    # in your release step, after migrate
```

See [schedules](schedules.md) for catch-up, overlap and time zones.

### 5. Run workers

```bash
python manage.py dewey run
```

Run as many copies as you need. Each serves the queues in `DEWEY["QUEUES"]`; run a
second service with other settings for queues that must not share capacity:

```bash
python manage.py dewey run --queues reports --concurrency 1
```

### 6. Check

```bash
python manage.py dewey doctor
python manage.py dewey queues
```

### Settings reference

All optional; shown with their defaults.

```python
DEWEY = {
    "DATABASE": "default",          # alias the worker and operator commands use
    "QUEUES": ("default",),         # queues `manage.py dewey run` serves
    "CONCURRENCY": 2,               # task processes per worker
    "AUTODISCOVER": "tasks",        # import <app>.tasks from every installed app
    "AUTO_APPLY_SCHEDULES": False,  # single-node installs: apply at worker start
    "ALERT_HOOK": None,             # "module:function", see operations
    "TIMING": {},                   # worker limits, e.g. {"task_timeout_seconds": 600}
    "START_METHOD": "spawn",        # or "fork": task processes share the imported app
    "TRACING": False,               # an OpenTelemetry span per attempt, see logging
    "FAILURE_DETAILS": "type",      # or "full": exception message and traceback, see logging
    "STARTUP_SECONDS": 30.0,        # budget for the app import and for each task process
    "SUPERVISION_SECONDS": 30.0,    # how long the service may go without progress
    "SHUTDOWN_SECONDS": 30.0,       # how long a stopping worker waits for running tasks
    "MAX_RESTARTS": 3,              # task-process crashes tolerated per service lifetime
}
```

An unknown key is an error at startup, so a typo cannot silently do nothing.

---

## SQLAlchemy (sync)

### 1. Install and create the schema

```bash
pip install "dewey[sqlalchemy]"
```

```python
from sqlalchemy import create_engine
from dewey.sqlalchemy import Base

engine = create_engine("postgresql://localhost/myapp")
Base.metadata.create_all(engine)  # or generate an Alembic revision from Base.metadata
```

`create_all()` creates missing tables; it never alters an existing one. When
upgrading, generate a migration from the metadata or apply the SQL in the upgrade
notes.

### 2. Declare tasks

```python
# myapp/tasks.py
import dewey

@dewey.task("orders.confirm", max_attempts=5)
def confirm_order(order_id: str) -> None:
    ...

@dewey.task("orders.expire_carts", schedule=dewey.cron("*/15 * * * *"))
def expire_carts() -> None:
    ...
```

### 3. Create work

```python
from sqlalchemy.orm import Session
from dewey.sqlalchemy import create_task

with Session(engine) as session, session.begin():
    order = Order(...)
    session.add(order)
    session.flush()
    create_task(session, task_type="orders.confirm", args=[str(order.id)])
```

`create_task` never commits: the task belongs to your transaction.

### 4. Write the worker's application factory

The worker calls this once in its parent process and once in each task process, so it
creates its own engine every time.

```python
# myapp/worker.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dewey.worker import WorkerContext


def create_application(context: WorkerContext):
    from myapp import tasks  # registers handlers and schedules

    engine = create_engine(os.environ["DATABASE_URL"], pool_size=1, max_overflow=0)
    return context.sqlalchemy(sessionmaker(engine), close_resources=engine.dispose)
```

### 5. Apply schedules, run workers, check

```bash
export DEWEY_APP=myapp.worker:create_application
dewey schedules apply      # release step
dewey run                  # as many copies as you need
dewey doctor
```

---

## SQLAlchemy (async)

```bash
pip install "dewey[async]"
```

Producers use the `_async` functions on an `AsyncSession`:

```python
from dewey.sqlalchemy import create_task_async

async with async_session() as session, session.begin():
    session.add(order)
    await session.flush()
    await create_task_async(session, task_type="orders.confirm", args=[str(order.id)])
```

The worker factory stays synchronous and hands over an async session factory; async
handlers are awaited on the task process's own event loop:

```python
# myapp/worker.py
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

def create_application(context):
    from myapp import tasks

    engine = create_async_engine(os.environ["DATABASE_URL"], pool_size=1, max_overflow=0)
    return context.async_sqlalchemy(async_sessionmaker(engine), close_resources=engine.dispose)
```

```python
@dewey.task("orders.confirm")
async def confirm_order(order_id: str) -> None:
    ...
```

An asyncpg deployment never needs a synchronous driver: the worker, the schedules and
the `dewey` command all run through the same factory.

---

## Sharing a database with your application

Dewey and your request handlers usually live on the same physical Postgres. That is
fine, as long as background work gets its own connection budget instead of competing
for your API's.

**Give the worker a small, hard-capped pool.** Each worker process needs one
connection at a time: the parent for ledger calls, and each task process for its
ledger calls plus whatever your handler opens. `pool_size=1, max_overflow=0` in the
factory above is the right default. `max_overflow=0` matters: with overflow allowed, a
burst of background work can open connections until Postgres refuses them, and the
process that gets refused is as likely to be a request handler as a worker.

A worker with `concurrency=4` therefore uses up to five connections for the ledger,
plus your handlers' own.

**Set server-side timeouts on the worker's role**, so a pathological query cannot hold
locks indefinitely:

```sql
ALTER ROLE dewey SET statement_timeout = '30s';
ALTER ROLE dewey SET lock_timeout = '5s';
ALTER ROLE dewey SET idle_in_transaction_session_timeout = '60s';
```

A separate role is worth it if you can manage one: it gives you a `CONNECTION LIMIT`
the workers physically cannot exceed, and makes their share legible in
`pg_stat_activity`. Individual tasks can also carry tighter budgets
(`statement_timeout_s`, `lock_timeout_s` on `@dewey.task`); see
[the worker](native-worker.md#time-limits-a-handler-can-rely-on).

**Producers always write on the business transaction's connection.** In Django,
`create_task` follows your normal ORM routing, and the worker reads the alias named
in `DEWEY["DATABASE"]`. Both must be the same database. Do not add a router that
sends Dewey's models to a different alias than the business write beside them: two
aliases are two connections and two transactions, even when they point at one
server, and the rollback guarantee silently disappears. `dewey doctor` and the Django
system checks warn when producers and the worker disagree.

---

## Testing handlers

Handlers are ordinary functions: call them directly in unit tests. To exercise the
ledger path without a worker, run one task inline:

```python
from dewey.django import create_task, process_task      # or dewey.sqlalchemy

task = create_task(task_type="orders.confirm", args=[str(order.id)])
assert process_task(task.id) is True
```

`process_task` is a test and eager-execution helper. It is not a production runner:
nothing recovers a row it leaves behind if the process dies. It does run the handler
the way the worker does: `dewey.current_task()` is bound, and the policy's
`statement_timeout_s`/`lock_timeout_s` are applied (to the Django connection; with
SQLAlchemy through `apply_task_timeouts()` in the handler, as in production).

A handler that is also called directly, outside any task, can ask
`dewey.current_task_or_none()` instead of catching `RuntimeError`:

```python
def moderate_photo(photo_id):
    task = dewey.current_task_or_none()
    if task is not None and task.remaining() < timedelta(seconds=10):
        raise dewey.RetryAfter(0)
    ...
```

## Next

- [Schedules](schedules.md): catch-up, overlap, plan/apply, daylight saving.
- [The worker](native-worker.md): settings, time limits, drain and exit statuses.
- [Operations](operations.md): the command, doctor, alerts.
- [Concepts](concepts.md): the state machine and what Dewey does and does not guarantee.
