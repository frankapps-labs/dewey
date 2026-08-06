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

Extras control *dependencies*, not which files ship: one wheel carries every module, so
`dewey/django/` is on disk (76 KB) even in a SQLAlchemy-only install. Nothing imports it
unless you ask for it — no import time, no memory, no Django dependency — and touching it
without Django installed tells you to install `dewey[django]`.

Pick a section by **ORM**, not by web framework. Dewey never imports or touches your web
layer — a FastAPI, Starlette, Litestar or Flask app on async SQLAlchemy follows
[SQLAlchemy (async)](#sqlalchemy-async) below, and one on sync SQLAlchemy follows
[SQLAlchemy (sync)](#sqlalchemy-sync). Only Django gets its own section, because Dewey
ships Django models, migrations and a management command.

---

## Django

### 1. Install and add the app

```bash
pip install "dewey[django,huey]"
```

`dewey[django]` brings Django, not a Postgres driver — Django needs one, so install it
if your project does not already have it:

```bash
pip install "psycopg[binary]"    # psycopg 3; or: pip install psycopg2-binary
```

Either driver works; the dispatcher's LISTEN wake-up supports both.

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

## Sharing a database with your application

Dewey and your request handlers usually live on the same physical Postgres. That is fine,
and it is the deployment the resilience lab exercises — but only if Dewey gets its own
connection budget rather than competing for your API's.

**Give Dewey a separate engine with a bounded pool.** The failure you are avoiding is a
dispatcher claim query or a batch of workers exhausting the pool your request handlers
need, turning background pressure into user-visible latency.

```python
# Your API keeps its own engine, sized for request concurrency.
api_engine = create_async_engine(DATABASE_URL, pool_size=10, max_overflow=5)

# Dewey gets its own, deliberately small and hard-capped.
dewey_engine = create_async_engine(
    DATABASE_URL,
    pool_size=2,
    max_overflow=0,   # a hard ceiling: never borrow from the headroom the API needs
    pool_timeout=5,   # fail fast instead of queueing behind background work
)
```

`max_overflow=0` is the important one. With overflow allowed, a burst of dispatcher and
worker activity can open connections without limit until Postgres refuses them — and the
process that gets refused is as likely to be a request handler as a worker.

**Budget one extra connection for LISTEN.** The dispatcher holds a dedicated connection
for the whole time it runs, because a connection parked in `LISTEN` cannot serve anything
else. It is taken outside the pool, so count it separately: one per dispatcher process.

**Set server-side timeouts on Dewey's role**, so a pathological query cannot hold locks
indefinitely:

```sql
ALTER ROLE dewey SET statement_timeout = '30s';
ALTER ROLE dewey SET lock_timeout = '5s';
ALTER ROLE dewey SET idle_in_transaction_session_timeout = '60s';
```

A separate database role is worth it if you can manage one: it gives you a
`CONNECTION LIMIT` Dewey physically cannot exceed, and makes Dewey's share legible in
`pg_stat_activity`.

For Django, point Dewey at its own alias and let the settings contract use it:

```python
DATABASES = {
    "default": {...},
    "dewey": {..., "CONN_MAX_AGE": 0, "OPTIONS": {"pool": {"max_size": 2}}},
}
DEWEY = {"DISPATCH": "myapp.tasks.adapter.dispatch", "DATABASE": "dewey"}
```

`DEWEY["DATABASE"]` must name the alias used by the dispatcher; the dispatcher does not
consult routers when its backend is constructed.

You will want a database router so Dewey's models resolve to that alias. Dewey resolves
the alias through your routers for every transaction, `SELECT FOR UPDATE`, write and
NOTIFY, so all of them stay on the one connection that holds the lock. If you prefer not
to add a router, `create_task`, `process_task`, `sweep`, `retry_task` and `kill_task`
also accept the alias explicitly: `create_task(task_type=..., using="dewey")`. The wider
query/action API (`get_*`, `bulk_retry`, `purge_completed`) follows routers and has no
`using` argument, so router-less alias use is limited to the functions listed above.
Pointing at the same physical database through a second alias is a legitimate
configuration — the point is the separate connection budget, not a separate server.

**What the lab measures.** Its `cohabitation` and `cohabitation-chaos` scenarios run
sustained tenant traffic (`SELECT pg_sleep`) against the same Postgres as a live
dispatcher, with an API pool of 10+5 and a Dewey pool of 2+0, and gate on four things: the
wake-up path still fires promptly, the worker still completes work, the API's own failure
ratio stays near zero, and cohabitation latency stays bounded. Both pass. Those pool
numbers are a reasonable starting point, not a universal answer — the shape to copy is
"small, hard-capped, and separate".

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
