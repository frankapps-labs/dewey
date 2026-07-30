# Getting started

Dewey needs three things running: your application (the producer), a dispatcher, and at
least one worker. The producer writes rows. The dispatcher claims them and hands IDs to
a transport. Workers process them.

This guide sets that up for Django, SQLAlchemy sync, and SQLAlchemy async.

## Before you start

- PostgreSQL 13+. Dewey uses `FOR UPDATE SKIP LOCKED`, partial indexes and
  LISTEN/NOTIFY; SQLite and MySQL are not supported for the ledger.
- Python 3.11+.
- A broker only if you want one. Huey over Redis is the supported transport; for a
  single-process setup you can dispatch straight into an in-process function.

---

## Django

### 1. Install and add the app

```bash
pip install "dewey[django,huey]"
```

```python
# settings.py
INSTALLED_APPS = [
    ...,
    "dewey.django",
]

DEWEY = {
    # Required: dotted path to your transport's dispatch callable.
    "DISPATCH": "myapp.tasks.adapter.dispatch",
}
```

```bash
python manage.py migrate
```

Migrations ship in the package, so that is all the schema setup there is.

### 2. Declare a task

Anywhere Django imports at startup — `myapp/tasks.py` is the convention.

```python
# myapp/tasks.py
import dewey
from myapp.models import Command

@dewey.task("agent.notify", max_attempts=5, backoff=dewey.Constant(3))
def notify_agent(command_id: str) -> None:
    command = Command.objects.get(id=command_id)
    if command.is_terminal:
        return  # someone already handled it; a no-op is success
    agent_channel.send(command.id)
```

The decorator returns your function untouched. It stays directly callable and directly
testable — `notify_agent("abc")` in a unit test runs the body with no framework
involved.

### 3. Wire the transport

Same module, so the worker and the dispatcher agree on the task name:

```python
# myapp/tasks.py (continued)
from huey import RedisHuey
from dewey.adapters.huey import HueyAdapter
from dewey.django import process_task

huey = RedisHuey("myapp", url="redis://localhost:6379/0")
adapter = HueyAdapter(huey)
adapter.register(process_task)
```

### 4. Create work

```python
from django.db import transaction
from dewey.django import create_task

with transaction.atomic():
    command = Command.objects.create(...)
    create_task(task_type="agent.notify", args=[str(command.id)])
```

If the block rolls back, the task row and its wake-up go with it. Postgres holds the
notification until commit, so there is no window in which a dispatcher can see work that
never happened — and no `on_commit` callback for you to remember.

### 5. Run the processes

```bash
python manage.py dewey_dispatcher      # claims rows, dispatches IDs, runs the sweep
huey_consumer myapp.tasks.huey         # processes task IDs
```

Both are ordinary long-running processes. Run more than one dispatcher if you like:
`SKIP LOCKED` makes them cooperate without a leader election.

Useful flags while getting oriented:

```bash
python manage.py dewey_dispatcher --once                     # one pass, then exit
python manage.py dewey_dispatcher --queues critical,default  # serve specific queues
python manage.py dewey_dispatcher --idle-poll 1              # tighter poll for local dev
```

### Settings reference

```python
DEWEY = {
    "DISPATCH": "myapp.tasks.adapter.dispatch",  # required
    "QUEUES": None,                    # None serves every queue
    "BATCH_SIZE": 100,                 # rows claimed per round trip
    "IDLE_POLL_SECONDS": 5.0,          # max wait before polling anyway
    "SWEEP_INTERVAL_SECONDS": 60.0,    # None disables recovery — see the warning below
    "DISPATCH_TIMEOUT_SECONDS": 300,   # must exceed your worst-case broker backlog
    "STUCK_THRESHOLD_MINUTES": 10,     # PROCESSING older than this is presumed abandoned
    "SWEEP_LIMIT": 100,
    "DATABASE": "default",             # alias, for a dedicated Dewey connection
}
```

A misspelled key raises `ImproperlyConfigured` at startup rather than being silently
ignored.

> **The dispatcher owns the sweep.** `FAILED → PENDING` is a sweep transition, so with
> `SWEEP_INTERVAL_SECONDS` set to `None` and nothing else calling `sweep()`, failed
> tasks never retry.

---

## SQLAlchemy (sync)

### 1. Create the schema

```python
from sqlalchemy import create_engine
from dewey.sqlalchemy import Base

engine = create_engine("postgresql://localhost/myapp")
Base.metadata.create_all(engine)  # or generate an Alembic revision from it
```

### 2. Declare a task and wire the transport

```python
# myapp/tasks.py
import dewey
from huey import RedisHuey
from sqlalchemy.orm import Session
from dewey.adapters.huey import HueyAdapter
from dewey.sqlalchemy import process_task

@dewey.task("invoice.send", max_attempts=3)
def send_invoice(invoice_id: int) -> None:
    with Session(engine) as session:
        invoice = session.get(Invoice, invoice_id)
        mailer.send(invoice.email, invoice.pdf_url)

huey = RedisHuey("myapp")
adapter = HueyAdapter(huey)

def _process(task_id: str) -> bool:
    with Session(engine) as session:
        return process_task(session, task_id)

adapter.register(_process)
```

Give the worker its own session. `process_task` commits at each phase of the two-phase
commit, so it must not share a session with request handling.

### 3. Create work

```python
from dewey.sqlalchemy import create_task

with Session(engine) as session:
    invoice = Invoice(...)
    session.add(invoice)
    session.flush()
    create_task(session, task_type="invoice.send", args=[invoice.id])
    session.commit()
```

### 4. Run the dispatcher

```python
# dispatcher.py
from dewey.dispatcher import Dispatcher
from dewey.sqlalchemy.dispatch import SQLAlchemyDispatchBackend
from myapp.tasks import adapter, engine

dispatcher = Dispatcher(SQLAlchemyDispatchBackend(engine), adapter.dispatch)
dispatcher.run()
```

For clean shutdown under a process manager:

```python
import signal
for name in ("SIGINT", "SIGTERM"):
    signal.signal(getattr(signal, name), lambda *_: dispatcher.stop())
dispatcher.run()
```

### Tuning

```python
Dispatcher(
    backend,
    adapter.dispatch,
    batch_size=100,
    idle_poll_seconds=5.0,      # LISTEN shortens this; it is not needed for correctness
    sweep_interval_seconds=60.0,
)

SQLAlchemyDispatchBackend(
    engine,
    queues=["critical"],        # None serves every queue
    dispatch_timeout_seconds=300,
    stuck_threshold_minutes=10,
)
```

---

## SQLAlchemy (async)

Producers and workers have async equivalents:

```python
import dewey
from dewey.sqlalchemy import create_task_async, process_task_async

@dewey.task("invoice.send", max_attempts=3)
async def send_invoice(invoice_id: int) -> None:
    async with AsyncSession(async_engine) as session:
        invoice = await session.get(Invoice, invoice_id)
        await mailer.send(invoice.email)

async with AsyncSession(async_engine) as session:
    await create_task_async(session, task_type="invoice.send", args=[invoice.id])
    await session.commit()
```

An async handler needs an async worker: register a `process_fn` that drives
`process_task_async` on your loop. Async handlers are awaited by `process_task_async`,
not by the sync `process_task`.

The dispatcher has an async twin, so an asyncpg-only deployment never needs a
synchronous driver:

```python
from dewey.dispatcher import AsyncDispatcher
from dewey.sqlalchemy.dispatch import AsyncSQLAlchemyDispatchBackend

dispatcher = AsyncDispatcher(
    AsyncSQLAlchemyDispatchBackend(async_engine),
    adapter.dispatch,          # sync or async callable, both work
)
await dispatcher.run()         # or asyncio.create_task(dispatcher.run())
```

It behaves identically to the sync dispatcher — same claim-commit-dispatch order, same
immediate release and backoff on transport failure, same sweep tick — and it wakes on
LISTEN through the asyncpg listener rather than a blocking thread. Cancelling the task
shuts it down cleanly; `stop()` lets it finish the current pass first.

---

## Without a broker

The transport is just a function that takes a task ID. In a single process, that can be
the worker itself:

```python
from dewey.dispatcher import Dispatcher
from dewey.sqlalchemy.dispatch import SQLAlchemyDispatchBackend

def dispatch_in_process(task_id: str) -> None:
    with Session(engine) as session:
        process_task(session, task_id)

Dispatcher(SQLAlchemyDispatchBackend(engine), dispatch_in_process).run()
```

You keep durability, retries, scheduling and the sweep. You give up parallelism and
process isolation, and a handler crash takes the dispatcher down with it. Fine for small
deployments and local development; add a broker when you want workers to scale or fail
independently.

---

## Verifying it works

```python
from dewey.django import get_stats  # or dewey.sqlalchemy.get_stats(session)

get_stats()
# {'pending': 3, 'dispatching': 1, 'processing': 2, 'completed': 4891, 'failed': 0, 'dead': 1}
```

Or straight from psql, which is rather the point of keeping the backlog in Postgres:

```sql
SELECT status, count(*) FROM task_entries GROUP BY status;
SELECT task_type, error, attempts FROM task_entries WHERE status = 'dead';
```

## Next

- [Concepts](concepts.md) — what each state means, how claims work, what Dewey does and
  does not guarantee
- [From Huey or Celery](onboarding/from-huey-celery.md) — migrating existing tasks
- [Query API](query-api.md) — operational queries and manual intervention
