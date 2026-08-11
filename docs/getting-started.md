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
    # Required: dotted path to a module-level dispatch callable.
    "DISPATCH": "dewey.contrib.django_huey.dispatch",
}
```

The value is resolved with Django's `import_string`, which imports a module and takes
one attribute from it. Object traversal such as `"myapp.tasks.adapter.dispatch"` —
module, then object, then method — is unsupported and fails at startup. Either use the
contrib module shown here, or export a module-level wrapper from your own module (step 3
shows one).

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

The preferred wiring for Django is `dewey.contrib.django_huey`. Point Huey's own Django
integration at your broker and Dewey at the contrib dispatch:

```python
# settings.py
HUEY = {
    "huey_class": "huey.RedisHuey",
    "name": "myapp",
    "url": "redis://localhost:6379/0",
}

DEWEY = {"DISPATCH": "dewey.contrib.django_huey.dispatch"}
```

Import the explicit wiring module from a task module Huey's normal Django discovery
already loads; Dewey does not perform hidden handler autodiscovery:

```python
# myapp/tasks.py
from dewey.contrib.django_huey import adapter, dispatch  # noqa: F401
```

Importing `dewey.contrib.django_huey` builds a `HueyAdapter` around
`huey.contrib.djhuey.HUEY`, registers Dewey's processing exactly once with Huey retries
disabled, and wraps worker execution in `huey.contrib.djhuey.close_db`. The module
exports `adapter` and `dispatch` at module level, and a misconfiguration (no Huey
installed, no `HUEY` setting) raises `ImproperlyConfigured` telling you what to fix,
rather than failing later in a worker.

`close_db` matters: a Huey worker is a long-lived process, and without it each worker
thread keeps reusing one Django connection forever. The first time Postgres drops that
connection — a restart, a failover, an idle timeout — every subsequent task on that
thread fails until the consumer restarts. `close_db` closes stale connections around
each task, the same lifecycle Django gives a request (and it honours `CONN_MAX_AGE`).

**Wiring it yourself.** If you manage your own Huey instance, apply `close_db` yourself
and export a module-level `dispatch` wrapper — `DEWEY["DISPATCH"]` cannot import a
method reached through an object (see step 1):

```python
# myapp/tasks.py (continued)
from huey.contrib.djhuey import HUEY, close_db
from dewey.adapters.huey import HueyAdapter
from dewey.django import process_task

adapter = HueyAdapter(HUEY)
adapter.register(close_db(process_task))

def dispatch(task_id: str):
    """Module-level, so DEWEY = {"DISPATCH": "myapp.tasks.dispatch"} can import it."""
    return adapter.dispatch(task_id)
```

### 4. Create work

```python
from django.db import transaction
from datetime import UTC, datetime, timedelta
from dewey.django import create_or_get_task

with transaction.atomic():
    command = Command.objects.create(...)
    create_or_get_task(
        task_type="agent.notify",
        idempotency_key=f"notify:{command.id}",
        args=[str(command.id)],
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
```

If the block rolls back, the task row and its wake-up go with it. Postgres holds the
notification until commit, so there is no window in which a dispatcher can see work that
never happened — and no `on_commit` callback for you to remember.

### 5. Run the processes

```bash
python manage.py dewey_dispatcher      # claims rows, dispatches IDs, runs the sweep
python manage.py run_huey              # processes task IDs
```

Both are ordinary long-running processes. Run more than one dispatcher if you like:
`SKIP LOCKED` makes them cooperate without a leader election.

Before calling the deployment ready, run the active doctor. Human output is the default;
JSON has stable finding IDs and exits non-zero on readiness errors:

```bash
python manage.py dewey_doctor
python manage.py dewey_doctor --format json --queues critical,default
```

It verifies configuration, PostgreSQL/schema/migrations, the importable dispatch callable,
the current process's handler/processor registration shape, aliases/recovery settings, and
a fresh matching dispatcher heartbeat. Without an expected-task manifest it cannot prove
that every intended handler was imported, and one process cannot inspect another worker's
in-memory registry; the output says so.

Useful dispatcher flags while getting oriented:

```bash
python manage.py dewey_dispatcher --once                     # one pass, then exit
python manage.py dewey_dispatcher --queues critical,default  # serve specific queues
python manage.py dewey_dispatcher --idle-poll 1              # tighter poll for local dev
```

### Settings reference

```python
DEWEY = {
    "DISPATCH": "dewey.contrib.django_huey.dispatch",  # required; module-level callable
    "QUEUES": None,                    # None serves every queue
    "BATCH_SIZE": 100,                 # rows claimed per round trip
    "IDLE_POLL_SECONDS": 5.0,          # max wait before polling anyway
    "SWEEP_INTERVAL_SECONDS": 60.0,    # None disables recovery — see the warning below
    "DISPATCH_TIMEOUT_SECONDS": 300,   # must exceed your worst-case broker backlog
    "STUCK_THRESHOLD_MINUTES": 10,     # PROCESSING older than this is presumed abandoned
    "SWEEP_LIMIT": 100,
    "DATABASE": "default",             # the dispatcher's alias
    "WORKER_DATABASE": None,           # worker alias; None lets Django routers decide
}
```

A misspelled key raises `ImproperlyConfigured` at startup rather than being silently
ignored.

> **Retries are direct; sweeps are recovery.** Due `FAILED` rows are claimable without a
> sweep and the dispatcher wakes for the earliest due row. Setting
> `SWEEP_INTERVAL_SECONDS=None` disables abandoned-processing/dispatch recovery and is
> reported by checks/doctor; it does not add ordinary retry latency.

---

## SQLAlchemy (sync)

### 1. Create the schema

```python
from sqlalchemy import create_engine
from dewey.sqlalchemy import Base

engine = create_engine("postgresql://localhost/myapp")
Base.metadata.create_all(engine)  # or generate an Alembic revision from it
```

`create_all()` does not alter an existing 0.4 table. Prefer generating an Alembic
revision from 0.5 metadata. The equivalent additive PostgreSQL DDL is:

```sql
ALTER TABLE task_entries ADD COLUMN IF NOT EXISTS expires_at timestamptz NULL;
ALTER TABLE task_entries ADD COLUMN IF NOT EXISTS expired_at timestamptz NULL;
ALTER TABLE task_entries ADD COLUMN IF NOT EXISTS initial_scheduled_for timestamptz NULL;
CREATE INDEX IF NOT EXISTS ix_task_entries_expires_at ON task_entries (expires_at)
  WHERE expires_at IS NOT NULL AND status IN ('pending', 'dispatching', 'failed');
CREATE TABLE IF NOT EXISTS dewey_dispatcher_heartbeats (
  instance_id varchar(36) PRIMARY KEY,
  dewey_version varchar(50) NOT NULL,
  backend varchar(50) NOT NULL,
  database varchar(200) NOT NULL,
  queues json NULL,
  started_at timestamptz NOT NULL,
  last_seen_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dewey_heartbeats_last_seen
  ON dewey_dispatcher_heartbeats (last_seen_at);
CREATE INDEX IF NOT EXISTS ix_dewey_heartbeats_backend_database
  ON dewey_dispatcher_heartbeats (backend, database);
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

For Django, name the roles explicitly. Dewey touches the database as three actors, and
they deliberately do not share connections:

- **Producer** — your application code calling `create_task` next to a business write.
  Atomicity comes first: the task must be written on the **same alias, and therefore the
  same connection and transaction, as the business row it belongs to**. Roll back, and
  neither ever existed; commit, and both did. Producer writes therefore follow your
  normal ORM routing — you do not point them anywhere special.
- **Dispatcher** — the `dewey_dispatcher` process. `DEWEY["DATABASE"]` names its alias.
  It opens its own connections and only ever sees rows after your transactions commit.
- **Worker** — `process_task` inside the Huey consumer. `DEWEY["WORKER_DATABASE"]`
  names its alias for the `dewey.contrib.django_huey` wiring (custom wiring passes
  `using=` instead). Also post-commit, also its own connections.

```python
DATABASES = {
    "default": {...},   # producers: business rows and their tasks, one transaction
    # Same NAME and HOST as "default" — same database, separate connection budget.
    "dewey": {..., "CONN_MAX_AGE": 0, "OPTIONS": {"pool": {"max_size": 2}}},
}
DEWEY = {
    "DISPATCH": "dewey.contrib.django_huey.dispatch",
    "DATABASE": "dewey",           # dispatcher
    "WORKER_DATABASE": "dewey",    # worker
}
```

`DEWEY["DATABASE"]` must name the alias used by the dispatcher; the dispatcher does not
consult routers when its backend is constructed.

Be clear about what the alias split does and does not buy. **Two aliases pointing at
one physical PostgreSQL database are two connections and two transactions.**
`transaction.atomic(using="default")` covers nothing written through `"dewey"`, even
though both aliases resolve to the same server and the same `task_entries` table. The
split exists to give background work its own connection budget; it never creates shared
atomicity.

Which is why you should **not add a database router that sends Dewey's models to the
background alias.** A router routes every `TaskEntry` write — including the producer's
`create_task` — so each task row silently moves onto the `"dewey"` connection, into its
own transaction, and commits or survives independently of the business write beside it.
The rollback guarantee from step 4 disappears, and nothing errors to tell you. Routers
*are* honoured consistently when you have one — Dewey resolves the alias through them
for every transaction, `SELECT FOR UPDATE`, write and NOTIFY, so everything stays on
the one connection that holds the lock — but routing the whole ledger away from
producers is a deliberate choice for a physically separate ledger database, made
knowing producer atomicity is lost. It is never a default.

For explicit pinning without a router, `create_task`, `process_task`, `sweep`,
`retry_task` and `kill_task` accept the alias directly:
`create_task(task_type=..., using="dewey")`. The wider query/action API (`get_*`,
`bulk_retry`, `purge_completed`) follows routers and has no `using` argument.

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
