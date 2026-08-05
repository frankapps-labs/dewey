# Dewey

**Guaranteed delivery engine for Python.** Postgres is the scheduler and the backlog;
your broker is just a worker pool.

Most task queues keep the backlog in Redis. A lost message is then lost work, a crashed
worker leaves nothing behind to explain itself, and "what is still pending for this
customer?" has no good answer. Dewey inverts that: a task is a Postgres row from the
moment it exists, a dispatcher hands ready rows to your broker, and the broker's only
job is carrying a task ID to a worker.

Losing the broker costs you latency. It cannot cost you work.

## Install

```bash
pip install dewey                        # core only
pip install "dewey[sqlalchemy]"          # SQLAlchemy models + sync API
pip install "dewey[sqlalchemy,async]"    # SQLAlchemy sync + async API
pip install "dewey[django]"              # Django models + API
pip install "dewey[huey]"                # Huey transport adapter
```

Requires Python 3.11+ and PostgreSQL 13+.

**Which extra do I need?** Dewey integrates at the database layer, not the web layer, so
pick by ORM:

| You use | Install | Why |
|---|---|---|
| Django | `dewey[django]` | Django models and migrations ship with it, plus a `dewey_dispatcher` management command |
| FastAPI, Starlette, Litestar, Flask + SQLAlchemy | `dewey[sqlalchemy,async]` (or drop `async` for sync) | Dewey never touches your web framework — it only needs your ORM |
| No web framework at all | `dewey[sqlalchemy]` | A script or worker fleet is a first-class consumer |

There is no `fastapi` extra because there is nothing for it to install: an async FastAPI
app is a SQLAlchemy async consumer, and that path has its own dispatcher
(`AsyncDispatcher`) so an asyncpg deployment never needs a synchronous driver.

## Quickstart

**1. Declare the task.** Handlers stay ordinary functions. Policy sits next to them, as
data.

```python
import dewey

@dewey.task("agent.notify", max_attempts=5, backoff=dewey.Constant(3))
def notify_agent(command_id: str) -> None:
    command = Command.objects.get(id=command_id)
    if command.is_terminal:
        return                                   # already handled; nothing to do
    try:
        agent_channel.send(command.id)
    except AgentOffline as exc:
        raise dewey.TransientError(str(exc))     # retry per policy
    except MalformedCommand as exc:
        raise dewey.NonRetryableError(str(exc))  # dead-letter now, don't burn attempts
```

**2. Create work inside your own transaction.** Producers never import handlers and
never touch the broker.

```python
from dewey.django import create_task

with transaction.atomic():
    command = Command.objects.create(...)
    create_task(task_type="agent.notify", args=[str(command.id)])
# Roll back, and neither the command nor the task ever existed.
```

**3. Run the dispatcher.** It claims ready rows, hands IDs to the transport, and runs
the recovery sweep.

```bash
python manage.py dewey_dispatcher
```

**4. Run a worker.** Ordinary Huey. The adapter is wired in a module both processes
import.

```python
# myapp/tasks.py
from huey import RedisHuey
from dewey.adapters.huey import HueyAdapter
from dewey.django import process_task

huey = RedisHuey("myapp")
adapter = HueyAdapter(huey)
adapter.register(process_task)
```

```bash
huey_consumer myapp.tasks.huey
```

```python
# settings.py
DEWEY = {"DISPATCH": "myapp.tasks.adapter.dispatch"}
```

That is the whole loop. SQLAlchemy — sync and async — works the same way; see
[docs/getting-started.md](docs/getting-started.md).

## What you get

- **A committed task is a task that will run.** The row and the wake-up commit together,
  so a rolled-back transaction leaves nothing behind and a committed one is never
  forgotten.
- **Retries, backoff and dead-lettering as policy**, resolved in one place instead of
  scattered across decorators and handler bodies. Handlers never sleep, never retry
  themselves.
- **Crash recovery you can reason about.** A dispatcher that dies mid-dispatch, a worker
  killed mid-task, a broker that drops a message: each is a state in the ledger, with a
  sweep that reclaims it.
- **A queryable backlog.** `SELECT count(*) FROM task_entries WHERE status = 'pending'`
  is the answer, not a Redis introspection script.
- **Duplicate delivery is harmless.** Claims are atomic, so a redelivered task ID is a
  logged no-op.
- **An audit trail**: attempts, errors, timestamps and correlation metadata per row.

## How it fits together

```text
  producer                  Postgres                 dispatcher            worker
 ─────────────────────────────────────────────────────────────────────────────────
  create_task()  ──────►  task_entries
                          (pending)
                             │  NOTIFY on commit
                             ▼
                          claim (SKIP LOCKED)  ◄────  LISTEN + poll
                          (dispatching)  ─────────►  dispatch(task_id) ──►  broker
                                                                              │
                          (processing)  ◄──────────────────────────────  process_task
                          (completed / failed / dead)
```

The state machine:

```text
PENDING ──► DISPATCHING ──► PROCESSING ──► COMPLETED
   ▲             │               │
   │             │               ├──► FAILED ──► PENDING   (retry, after backoff)
   │             │               │         └───► DEAD      (attempts exhausted)
   └─────────────┴───────────────┘                          (timeout sweep)
```

`PENDING → PROCESSING` is also legal, for in-process execution with no broker in the
path. `DEAD → PENDING` is a manual retry.

## Operational notes

- **The dispatcher must be running for retries to happen.** `FAILED → PENDING` is a
  sweep transition, and the dispatcher owns the sweep tick.
- **Polling is the correctness path; LISTEN is an optimisation.** The dispatcher polls
  regardless, which is what recovers missed notifications and newly-due scheduled work.
- **`dispatch_timeout_seconds` must exceed your worst-case broker backlog**, or work
  that is merely waiting gets reclaimed and dispatched twice.
- **Dewey owns retry, not your broker.** The Huey adapter registers with `retries=0` on
  purpose: two retry engines over one task is how work runs twice.
- **Give Dewey its own bounded connection pool** when it shares a database with your
  request handlers, so background pressure cannot become user-visible latency. See
  [sharing a database](docs/getting-started.md#sharing-a-database-with-your-application).

## Documentation

| Guide | What it covers |
|---|---|
| [Getting started](docs/getting-started.md) | SQLAlchemy sync, SQLAlchemy async, and Django, end to end, plus sharing a database with your app |
| [Concepts](docs/concepts.md) | States, claims, policy resolution, and the limits of what Dewey guarantees |
| [Adapters](docs/adapters.md) | The transport contract, and writing your own |
| [From Huey or Celery](docs/onboarding/from-huey-celery.md) | Pattern-by-pattern migration, one task type at a time |
| [Query API](docs/query-api.md) | Backlog, stuck work, dead letters, manual retry, purging |
| [Logging](docs/logging.md) | Correlation metadata across producer, dispatcher and worker |

## Stability

On `0.x` the public API may change between minor versions. `1.0` waits until the API has
been proven by real production use.

The release is deliberately one thing: durable task delivery. Multi-channel notification
delivery is **not** part of it — an earlier ledger for that was removed before publishing
rather than shipped half-committed, and the shape it should return in is a task type with
a channel handler.

## Development

```bash
make install           # install with dev dependencies
make up                # Postgres + Redis for the test suite
make test-integration  # run the suite against them
make wheel-smoke       # build a wheel and exercise it in a clean venv
make lint typecheck    # ruff + basedpyright
make down
```

The suite runs against real Postgres by design: `FOR UPDATE SKIP LOCKED`, partial
indexes, LISTEN/NOTIFY and committed claims cannot be proven against a fake.

## Acknowledgements

Thanks to [Chad Whitacre](https://github.com/chadwhitacre), the original owner of the `dewey` PyPI project, for kindly donating the package name.

## License

MIT — see [LICENSE](LICENSE).
