# Dewey

**Guaranteed delivery engine for Python.**

Your task queue (Huey, Celery) is the fast path. Postgres is the guarantee. Dewey writes every task to a database ledger before it is enqueued, then uses retry, sweep, and dead-letter handling to make background work auditable and recoverable.

## Why

Every production system with background tasks eventually needs the same safety net:

- durable task records in Postgres
- broker enqueue as a fast path, not the source of truth
- worker state transitions with attempts and errors recorded
- periodic sweep for broker drops and worker crashes
- dead-lettering when retry budgets are exhausted

Dewey packages that pattern as a small Python library.

## Install

```bash
pip install dewey                        # core only
pip install "dewey[sqlalchemy]"          # SQLAlchemy models + sync API
pip install "dewey[sqlalchemy,async]"    # SQLAlchemy sync + async API
pip install "dewey[django]"              # Django models + API
pip install "dewey[huey]"                # Huey adapter
pip install "dewey[celery]"              # Celery adapter
```

## Minimal SQLAlchemy example

```python
from sqlalchemy.orm import Session

from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.sweep import sweep


def handle_task(task_type: str, payload: dict) -> None:
    if task_type == "order.confirmed":
        send_confirmation_email(payload["order_id"])


# Producer: write to the ledger, then enqueue the task ID.
with Session(engine) as session:
    task = create_task(
        session,
        task_type="order.confirmed",
        payload={"order_id": "ORD-123"},
    )
    session.commit()
    adapter.enqueue(task.id)


# Worker: claim, run, and record completion/failure.
def worker_process(task_id: str) -> None:
    with Session(engine) as session:
        process_task(session, task_id, handler=handle_task)


# Periodic safety net: retry ready failures and unstuck abandoned work.
with Session(engine) as session:
    result = sweep(session)
    session.commit()
    for task_id in result["failed"] + result["stuck"]:
        adapter.enqueue(task_id)
```

## Core guarantees

- Tasks are stored before broker enqueue.
- Workers use explicit state transitions: `pending → processing → completed/failed/dead`.
- Failed tasks retry after backoff until `max_attempts` is reached.
- Stuck `processing` tasks are swept back to `pending`, or to `dead` if attempts are exhausted.
- Query APIs expose pending, processing, failed, stuck, and dead-lettered work.

## Documentation

- [Getting started](docs/getting-started.md) — SQLAlchemy, async, and Django walkthroughs
- [Concepts](docs/concepts.md) — state machine, sweep, backoff, and design rationale
- [Queue adapters](docs/adapters.md) — Huey and Celery integration
- [Notifications](docs/notifications.md) — tracked email/webhook/Slack-style delivery
- [Logging and trace context](docs/logging.md) — correlate producer and worker logs
- [Query API](docs/query-api.md) — dashboard and admin helpers

## Acknowledgements

Thanks to [Chad Whitacre](https://github.com/chadwhitacre), the original owner of the `dewey` PyPI project, for kindly donating the package name.

## License

MIT — see [LICENSE](LICENSE).
