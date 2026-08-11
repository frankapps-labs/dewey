# Transport adapters

An adapter carries a task ID from the dispatcher to a worker. That is all it does. It
has no say in when work runs, how often it retries, or what happens when it fails —
those live in Postgres, as policy.

## The contract

```python
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class DispatcherAdapter(Protocol):
    def register(self, process_fn: Callable[[str], Any]) -> None: ...
    def dispatch(self, task_id: str) -> Any: ...
```

- **`register(process_fn)`** — called once per process, at import time. Binds the
  callable that will receive a task ID and process it.
- **`dispatch(task_id)`** — called only by the dispatcher, only after the claim is
  committed. Must be safe to call from several dispatchers at once, and must not block
  on the task completing.

`isinstance(adapter, DispatcherAdapter)` works at wiring time, though a runtime check
only verifies that the methods exist. `tests/test_adapter_protocol.py` locks the
signatures.

There is no `enqueue()`. Producers do not talk to a broker at all.

## Huey

```python
# myapp/tasks.py — imported by both the worker and the dispatcher
from huey import RedisHuey
from dewey.adapters.huey import HueyAdapter
from dewey.django import process_task   # or a session-wrapping fn for SQLAlchemy

huey = RedisHuey("myapp", url="redis://localhost:6379/0")
adapter = HueyAdapter(huey)
adapter.register(process_task)
```

```bash
huey_consumer myapp.tasks.huey          # worker
python manage.py dewey_dispatcher       # dispatcher
```

Both processes import the module, so both agree on the registered task name. The
dispatcher never imports your handlers — only the adapter wiring.

### Retries stay with Dewey

`register()` registers the Huey task with `retries=0`, deliberately. Two retry engines
over one task is how work runs twice and how attempt counters stop meaning anything.
Dewey schedules retries via `scheduled_for` and the sweep; Huey just delivers.

### What a broker outage looks like

`dispatch()` raising is normal and handled: the dispatcher returns the claimed rows to
`PENDING` without consuming an attempt, backs off (1s doubling to 30s), and retries. The
backlog is in Postgres, so nothing is lost — only delayed.

The first failure in a batch abandons the rest of that batch. One transport error is
nearly always broker-wide, and pushing the remaining IDs at a dead broker would only
delay their release.

### Queues and priority

Dewey's `queue` and `priority` are resolved in the claim query, not by Huey. Scope a
dispatcher to particular queues and run one process per lane:

```bash
python manage.py dewey_dispatcher --queues critical
python manage.py dewey_dispatcher --queues default,bulk
```

Priority ordering (`priority DESC`) applies within a claim batch. It is not a preemption
mechanism: work already dispatched is not reordered.

## No broker at all

The transport is a function taking a task ID, so it can be the worker:

```python
def dispatch_in_process(task_id: str) -> None:
    with Session(engine) as session:
        process_task(session, task_id)

Dispatcher(SQLAlchemyDispatchBackend(engine), dispatch_in_process).run()
```

Durability, retries, scheduling and the sweep all still work. You give up parallelism and
process isolation, and a handler crash takes the dispatcher with it.

## Writing an adapter

RQ, dramatiq, SQS, a thread pool — anything that can carry a string:

```python
class MyAdapter:
    def __init__(self, client) -> None:
        self._client = client
        self._process_fn = None

    def register(self, process_fn) -> None:
        self._process_fn = process_fn
        self._client.subscribe("dewey", lambda message: process_fn(message.body))

    def dispatch(self, task_id: str) -> None:
        # Raise if the transport is unreachable: the dispatcher will return the row
        # to PENDING and retry. Do not swallow the error — silence would look like a
        # successful dispatch and the row would sit in DISPATCHING until the timeout.
        self._client.publish("dewey", task_id)
```

Two rules:

1. **Raise on failure.** That is how the dispatcher learns to release the claim and back
   off. Swallowing the error strands the row until `dispatch_timeout_seconds`.
2. **Do not add retries.** If your transport retries on its own, turn it off. Dewey owns
   the attempt budget.

At-least-once delivery is fine. Claims are atomic, so a redelivered ID is a logged no-op.

## Celery

Not supported in this release. Huey is the transport that gets integration coverage, and
advertising untested parity would be worse than saying so plainly. Celery's `.delay()`
call sites map cleanly onto `create_task` — see
[from-huey-celery.md](onboarding/from-huey-celery.md) — and a Celery adapter is a small
piece of work against the contract above if you want one.
