# Dewey

[![CI](https://github.com/frankapps-labs/dewey/actions/workflows/ci.yml/badge.svg)](https://github.com/frankapps-labs/dewey/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/frankapps-labs/dewey/badge.svg?branch=main)](https://coveralls.io/github/frankapps-labs/dewey?branch=main)
[![PyPI](https://img.shields.io/pypi/v/dewey.svg)](https://pypi.org/project/dewey/)
[![Python](https://img.shields.io/pypi/pyversions/dewey.svg)](https://pypi.org/project/dewey/)

**Guaranteed task delivery for Python, on Postgres.** Durable tasks, schedules and a
worker in one library. No broker.

Most task queues keep the backlog in a broker and the clock in a separate scheduler.
A lost message is then lost work, a scheduler that was down at 03:15 leaves no trace
that anything was missed, and "what is still pending for this customer?" has no good
answer. Dewey keeps all of it in the database you already run:

- **A task is a Postgres row from the moment it exists**, created in your own
  transaction. Commit, and it will run. Roll back, and it never existed.
- **A scheduled run is a row too**: one per tick, created exactly once however many
  workers you run, and on record even when it was missed.
- **One process type runs everything.** `dewey run` claims, executes, retries,
  recovers and keeps the clock. Scale it by count.

## Install

```bash
pip install "dewey[django]"              # Django models, migrations and manage.py dewey
pip install "dewey[sqlalchemy]"          # SQLAlchemy models, sync and async API
pip install "dewey[async]"               # the same, plus the asyncpg driver
```

Requires Python 3.11+ and PostgreSQL 13+. Dewey integrates at the database layer, so
pick by ORM: a FastAPI, Starlette, Litestar or Flask application is a SQLAlchemy
consumer, and a script with no web framework is a first-class one.

## Quickstart

**1. Declare the task.** Handlers stay ordinary functions. Policy sits next to them, as
data.

```python
# myapp/tasks.py
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

**2. Create work inside your own transaction.** Producers never import handlers.

```python
from dewey.django import create_task

with transaction.atomic():
    command = Command.objects.create(...)
    create_task(task_type="agent.notify", args=[str(command.id)])
# Roll back, and neither the command nor the task ever existed.
```

**3. Schedule what should happen on a clock.**

```python
@dewey.task("sessions.purge", schedule=dewey.cron("15 3 * * *", tz="Europe/London"))
def purge_expired_sessions() -> None:
    ...
```

```bash
python manage.py dewey schedules apply     # in your release step
```

**4. Run workers.**

```bash
python manage.py dewey run
```

**5. See what is going on.**

```bash
python manage.py dewey queues
python manage.py dewey dlq list
python manage.py dewey doctor
```

That is the whole loop. SQLAlchemy, sync and async, works the same way with
`dewey run --app myapp.worker:create_application`; see
[getting started](https://github.com/frankapps-labs/dewey/blob/main/docs/getting-started.md).

## What you get

- **A committed task is a task that will run.** There is no second system to hand it
  to, so there is no window in which it can be lost.
- **Retries, backoff and dead-lettering as policy**, resolved in one place. Handlers
  never sleep and never retry themselves.
- **Schedules with a memory.** Cron expressions and intervals declared in code, with a
  per-schedule answer to "what if we were down?" (`skip`, `once`, `all`) and "what if
  the last run is still going?". Missed ticks are counted and alertable.
- **Crash recovery you can reason about.** Every claim is a lease in the ledger. A
  worker killed mid-task loses the lease and another worker retries the task.
- **Time limits.** A hard timeout per task, a deadline the handler can read, and
  per-task database statement and lock timeouts.
- **A queryable backlog.** `dewey queues`, or
  `SELECT count(*) FROM task_entries WHERE status = 'pending'`.
- **Operations built in.** One command (and the JSON API under it) for queues, tasks,
  dead letters, schedules and a readiness check; one hook for alerts on dead work,
  recovered work and missed ticks.
- **Idempotent creation.** `create_or_get_task()` deduplicates on a key and raises
  `IdempotencyConflictError` on conflicting reuse.
- **Trace context carried across**: the creator's correlation IDs (and W3C
  `traceparent`) are restored around the handler.

## How it fits together

```text
  producer                    Postgres                       dewey run (N copies)
 ───────────────────────────────────────────────────────────────────────────────────
  create_task()  ──────►   task_entries  (pending)
                                │
  schedules ── due tick ──►     │   one row per tick
                                ▼
                           claim, FOR UPDATE SKIP LOCKED  ◄──── worker parent
                           (dispatching: leased)
                                │
                           (processing)  ◄──── task process runs the handler
                                │
                           (completed / failed → retry / dead)
```

The state machine:

```text
PENDING ──► DISPATCHING ──► PROCESSING ──► COMPLETED
   ▲             │               │
   │             │               ├──► FAILED ──► DISPATCHING (retry when due)
   │             │               │         └───► DEAD        (attempts exhausted)
   └─────────────┴───────────────┘                            (lease expired: recovered)
     └──────────────► EXPIRED ◄──────────────┘                (deadline before start)
```

`DEAD → PENDING` is a manual retry.

## Operational notes

- **Run it in a container.** The worker isolates each task in its own process and
  needs an enclosing kill-all boundary; any container runtime provides one.
- **Handlers must be idempotent.** Delivery is at-least-once: a worker killed between
  a side effect and its commit means the task runs again.
- **Apply schedules in your release step.** Workers never write schedule definitions,
  so a rolling deploy cannot make old and new code fight over them. `dewey doctor`
  tells you when you forgot.
- **Give Dewey its own bounded connection pool** when it shares a database with your
  request handlers. See
  [sharing a database](https://github.com/frankapps-labs/dewey/blob/main/docs/getting-started.md#sharing-a-database-with-your-application).
- **Producers write on the business transaction's connection.** `create_task` must use
  the same database alias as the business write it belongs to; that is what makes the
  rollback guarantee real.

## Documentation

| Guide | What it covers |
|---|---|
| [Getting started](https://github.com/frankapps-labs/dewey/blob/main/docs/getting-started.md) | Django, SQLAlchemy sync and async, end to end |
| [Concepts](https://github.com/frankapps-labs/dewey/blob/main/docs/concepts.md) | States, claims and leases, policy, and the limits of what Dewey guarantees |
| [Schedules](https://github.com/frankapps-labs/dewey/blob/main/docs/schedules.md) | Cron and intervals, plan/apply, catch-up and overlap, daylight saving |
| [The worker](https://github.com/frankapps-labs/dewey/blob/main/docs/native-worker.md) | `dewey run`, settings, time limits, drain and exit statuses |
| [Operations](https://github.com/frankapps-labs/dewey/blob/main/docs/operations.md) | The `dewey` command, doctor, alerts and the operations API |
| [Idempotency](https://github.com/frankapps-labs/dewey/blob/main/docs/idempotency.md) | Creation dedupe, stable handler effect keys, and what stays your job |
| [Query API](https://github.com/frankapps-labs/dewey/blob/main/docs/query-api.md) | Backlog, dead letters, manual retry, purging |
| [Logging](https://github.com/frankapps-labs/dewey/blob/main/docs/logging.md) | Correlation metadata from producer to handler |
| [Pull runners](https://github.com/frankapps-labs/dewey/blob/main/docs/pull-runners.md) | Serving workers in other languages through your own gateway |
| [Upgrading to 0.6](https://github.com/frankapps-labs/dewey/blob/main/docs/upgrading-0.6.md) | From the broker-and-dispatcher shape of 0.5 |

## Stability

On `0.x` the public API may change between minor versions. `1.0` waits until the API has
been proven by real production use.

Dewey does one thing: durable background work on Postgres, from the row to the clock
to the runner. Multi-channel notification delivery is not part of it; notification
tooling can build channel handlers on ordinary Dewey tasks.

## Releases

Dewey ships small, often: **the default is a micro-release on every behavior-changing
merge.** Each such merge bumps the version (SemVer — patch = fix, minor = feature, major =
breaking), adds a dated `## [x.y.z] - date` entry to
[`CHANGELOG.md`](https://github.com/frankapps-labs/dewey/blob/main/CHANGELOG.md), and tags
`v0.x.y`. CI/chore-only changes (workflows, dependency admission, builds) do **not** get a
version-tagged release — they fold into the next real one. A **user-facing security or
dependency-floor change is a release**, not an `[Unreleased]` entry — it bumps the version
exactly like a feature.

Two things to know when reading the log:

- A **`## [Unreleased]` section is the deliberate exception, not the norm**, used only when
  several changes must land together as one packet.
- **A changelog entry is not a release.** An entry may sit on `main` untagged until the next
  release is cut; the changelog records the release lineage, it does not itself ship code.

## Development

```bash
make install           # install with dev dependencies
make up                # Postgres for the test suite
make test-integration  # run the suite against them
make wheel-smoke       # build a wheel and exercise it in a clean venv
make lint typecheck    # ruff + basedpyright
make down
```

The suite runs against real Postgres by design: `FOR UPDATE SKIP LOCKED`, partial
indexes and committed claims cannot be proven against a fake.

## Acknowledgements

Thanks to [Chad Whitacre](https://github.com/chadwhitacre), the original owner of the `dewey` PyPI project, for kindly donating the package name.

## License

MIT — see [LICENSE](https://github.com/frankapps-labs/dewey/blob/main/LICENSE).
