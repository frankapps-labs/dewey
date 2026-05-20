# Queue adapters

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
