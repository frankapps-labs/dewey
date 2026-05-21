# Queue adapters

## Upcoming: `DispatcherAdapter` protocol

The long-term direction is **Dewey-driven dispatch**: producers only write
rows, and a Dewey dispatcher claims them and hands the task ID to a
transport adapter that conforms to `dewey.adapters.DispatcherAdapter`:

```python
from dewey.adapters import DispatcherAdapter, ProcessTaskFn

class MyAdapter:
    def register(self, process_fn: ProcessTaskFn) -> None:
        """Wire the worker-side processor (called once per worker)."""

    def dispatch(self, task_id: str) -> None:
        """Hand a claimed task ID to the transport's worker pool."""

assert isinstance(MyAdapter(), DispatcherAdapter)  # runtime_checkable
```

The Huey and Celery adapters below still expose the legacy `setup()` +
`enqueue()` API. They will gain `DispatcherAdapter` conformance in a
follow-up; the protocol is published now so external adapters can target
it. See the protocol docstring for the full lifecycle contract.

## Huey Adapter

Works with both SQLAlchemy and Django:

```python
from huey import RedisHuey
from dewey.adapters.huey import HueyAdapter

huey = RedisHuey("myapp")
adapter = HueyAdapter(huey)

adapter.setup(
    process_fn=worker_process,    # your function that calls process_task
    sweep_fn=sweep_fn,            # your function that calls sweep + re-enqueues
    sweep_interval_minutes=5,
)

# Then enqueue tasks:
adapter.enqueue(task_id="abc-123")
```

## Celery Adapter

Works with both SQLAlchemy and Django:

```python
from celery import Celery
from dewey.adapters.celery import CeleryAdapter

app = Celery("myapp", broker="redis://localhost:6379/0")
adapter = CeleryAdapter(app)

adapter.setup(
    process_fn=worker_process,    # your function that calls process_task()
    sweep_fn=sweep_fn,            # your function that calls sweep() + re-enqueues
    sweep_interval_seconds=300,   # default: 5 min (registered via beat_schedule)
)

# Enqueue tasks — Celery supports queue routing and priority natively:
adapter.enqueue(task_id="abc-123", queue="critical", priority=9)
```

Run celery beat alongside your worker to enable the periodic sweep:

```bash
celery -A myapp beat
```
