# Logging and trace context

## What Dewey logs, and where

| Logger | Emits |
|---|---|
| `dewey.worker` | worker start and stop, schedule drift at startup, supervision faults (never payloads or raw database errors) |
| `dewey.sqlalchemy.executor` / `dewey.django.executor` | task created; for inline execution: claimed, completed, failed with attempt counts and the next retry time |
| `dewey.alerts` | an alert hook that raised |

Every task-level line carries `id=` and `type=`. What happened to a task in a worker
is in the ledger rather than the log: `dewey tasks show <id>` prints its history, and
the [alert hook](operations.md#alerts) tells you about the outcomes that need a person.

## Logging & trace context

Dewey is a library, not a framework — it never configures logging. Every
Dewey log goes through a logger under the `dewey.*` namespace
(`dewey.sqlalchemy.executor`, `dewey.django.executor`, etc.), so you
wire it up like any other library:

```python
# Django LOGGING dict
LOGGING = {
    "version": 1,
    "loggers": {"dewey": {"level": "INFO", "handlers": ["console"]}},
    ...
}

# stdlib
import logging
logging.getLogger("dewey").setLevel("INFO")
```

### Trace context across the worker boundary

Every production system with background tasks hits the same correlation
problem: a user request creates a task, a worker (different process, later
time) runs the task, the handler raises — and you can't tie the error back
to the originating request.

Dewey solves this with a library-agnostic carrier: a `ContextVar[dict]`
that round-trips through `metadata["trace"]` on the task row.

```python
from dewey.core.logging import (
    TraceContextFilter,    # logging.Filter for the consumer side
    set_trace_context,     # producer-side: set context (or use middleware)
    reset_trace_context,   # producer-side: pop context
)
```

**Producer (your API/request layer):**

```python
# FastAPI middleware (or DRF middleware, or anywhere)
@app.middleware("http")
async def request_id_middleware(request, call_next):
    token = set_trace_context({"request_id": uuid.uuid4().hex})
    try:
        return await call_next(request)
    finally:
        reset_trace_context(token)

# Anywhere below it: the context is captured for you.
await create_task_async(session, task_type="order.confirmed", args=[order_id])
```

`create_task` stores the current trace context on the row. If OpenTelemetry is
installed and a span is active, its W3C `traceparent` is stored too; nothing else is
required, and OpenTelemetry is never a dependency.

**Consumer (your logging config):**

```python
handler = logging.StreamHandler()
handler.addFilter(TraceContextFilter())
handler.setFormatter(
    logging.Formatter(
        "%(asctime)s [req=%(dewey_request_id)s] %(name)s %(message)s",
        defaults={"dewey_request_id": "-"},
    )
)
```

The worker restores `metadata["trace"]` around your handler, so every log line the
handler produces is tagged with the originating request. When the row carries a
`traceparent` and OpenTelemetry is installed in the worker, spans the handler starts
join the creator's trace. Workers in other languages receive the same dict in the
[pull-runner](pull-runners.md) invocation.

JSON formatters (logfire, python-json-logger, structlog renderers) pick up
`record.dewey_trace` (full dict) and `record.dewey_<field>` (flattened)
automatically.

The carrier is intentionally agnostic: put a W3C `traceparent`, an OTel
context, an AWS X-Ray ID, a plain `request_id`, or all of the above.
Dewey round-trips whatever you give it.
