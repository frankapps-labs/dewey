# Logging and trace context

## What Dewey logs, and where

| Logger | Emits |
|---|---|
| `dewey.worker` | worker start and stop, schedule drift at startup, supervision faults (never payloads or raw database errors); every failed attempt, see below |
| `dewey.sqlalchemy.executor` / `dewey.django.executor` | task created; for inline execution: claimed, completed, failed with attempt counts and the next retry time |
| `dewey.alerts` | an alert hook that raised |

Every task-level line carries `id=` and `type=`. What happened to a task is in the
ledger as well as the log: `dewey tasks show <id>` prints its history, and the
[alert hook](operations.md#alerts) tells you about the outcomes that need a person.

### Failed attempts

Every attempt that fails is one log record on `dewey.worker`:

- **WARNING** when the task will be retried (`reason=retry`, with `retry_at`), and
  when a worker died or hit the task's time limit and the attempt was recovered
  (`reason=interrupted` or `timed_out`).
- **ERROR** for the attempt that leaves the task dead (`reason=attempts exhausted`
  or `non-retryable`, or a recovery that used the last attempt).

By default the record names the exception's **type** only (`error=ValueError`): an
exception's message can quote handler input or SQL parameters, and a log collector
is not behind the ledger's `--reveal` boundary. With `failure_details: "full"`
(`DEWEY["FAILURE_DETAILS"] = "full"`) the record made in the process that ran the
handler also carries the message and the traceback (`exc_info`), so a logging-based
error tracker (Sentry's logging integration, Logfire, python-json-logger) shows the
traceback and groups by it; choose it when the log pipeline scrubs what it collects.
The row's stored error is the same either way. A recovery is logged by the worker
that noticed the lost execution, which has the row but not the exception. Each
record also carries the task's identity as structured fields on the record, for JSON
handlers and for counting: `task_id`, `task_type`, `queue`, `attempt`,
`max_attempts`, `outcome` (`failed` or `dead`) and `reason`. Match on those fields
rather than on the message text, which may change between releases. An operator's
`tasks kill` is not logged as a failure.

The inline `process_task` helpers log the same record, on their own logger.

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

## OpenTelemetry spans

Carrying the `traceparent` makes the handler's own spans join the request's trace.
With tracing turned on, the worker also opens **one span per attempt**, so a trace
shows the task itself: when it ran, how long it took, which attempt it was and why
it failed. Logfire, Sentry, Jaeger and anything else that speaks OpenTelemetry show
it under the request that created the task.

Install the API and turn tracing on in the worker:

```bash
pip install 'dewey[otel]'
```

```python
DEWEY = {"TRACING": True}        # Django
```

```json
{"factory": "myapp.worker:create_application", "tracing": true}
```

Dewey depends on `opentelemetry-api` only, and only when this is on (a worker told
to trace without it refuses to start). Your application configures the SDK or
vendor distribution that exports the spans, in the worker process exactly as in
the web process: the factory, or Django's `settings`, runs in every task process.
With Logfire on Django, for example:

```python
# settings.py: runs in the web process and in every dewey run process
import logfire

logfire.configure(service_name="my-app")   # LOGFIRE_TOKEN from the environment
logfire.instrument_django()

DEWEY = {"TRACING": True, "START_METHOD": "spawn"}
```

Each attempt is a span named `dewey task <type>` of kind `CONSUMER` with
`messaging.system = "dewey"`, `messaging.destination.name` (the queue),
`messaging.message.id` and `dewey.task.id`, `dewey.task.type`, `dewey.task.queue`,
`dewey.task.attempt` and `dewey.task.max_attempts`. It is:

- **a child of the span that created the task** when the row carries a
  `traceparent`, so the request and every attempt of its task are one trace. A
  retry hours later is still a span in that trace, with its attempt number; a
  trace that was not sampled at the request stays unsampled in the worker. A task
  with no creator trace, such as a schedule tick, starts a trace of its own.
- **the current span while the handler runs**: spans the handler starts, and the
  spans of instrumented libraries it calls (a database driver, `boto3`, `httpx`),
  nest under it, so a photo task shows its decode, its moderation call and its
  save as children.
- **ended with the exception recorded and the status set to error** when the
  handler raised. The [failed-attempt log record](#failed-attempts) is made inside
  the span, so a log-and-trace backend links the two.

A task process flushes the tracer provider when the worker stops it (a
`multiprocessing` child runs no `atexit` hooks, so a batch exporter's buffer would
otherwise be lost on a graceful stop). Idle task processes get `termination_seconds`
(4 s by default), together, to finish that flush before they are terminated, and
the flush is bounded by it; an exporter that needs longer at shutdown needs a
larger `timing.termination_seconds`. A process killed outright, after its task
ran past its time limit, cannot flush; the worker parent then logs the recovered
attempt itself. With `start_method: "fork"`, the task processes inherit the
provider the launching process configured; the OpenTelemetry SDK's batch processor
restarts its export thread after a fork, and Dewey's test suite verifies spans
exported from forked task processes.
