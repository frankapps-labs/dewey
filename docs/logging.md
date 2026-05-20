# Logging and trace context

## Logging & trace context

Dewey is a library, not a framework — it never configures logging. Every
Dewey log goes through a logger under the `dewey.*` namespace
(`dewey.sqlalchemy.executor`, `dewey.django.notifications`, etc.), so you
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
that round-trips through `metadata["trace"]` on the task/notification row.

```python
from dewey.core.logging import (
    TraceContextFilter,    # logging.Filter for the consumer side
    bind_to_metadata,      # producer-side: snapshot current context
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

# Webhook handler
await create_task_async(
    session,
    task_type="order.confirmed",
    payload=body,
    metadata=bind_to_metadata({"source": "webhook"}),  # captures request_id
)
```

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

Dewey automatically restores `metadata["trace"]` into the contextvar before
invoking your handler, so every log line produced during Phase 2 (handler)
and Phase 3 (status writes) is tagged with the originating request_id.

JSON formatters (logfire, python-json-logger, structlog renderers) pick up
`record.dewey_trace` (full dict) and `record.dewey_<field>` (flattened)
automatically.

The carrier is intentionally agnostic: put a W3C `traceparent`, an OTel
context, an AWS X-Ray ID, a plain `request_id`, or all of the above.
Dewey round-trips whatever you give it.
