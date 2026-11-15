"""Logging + trace-context helpers.

Dewey is a library, not a framework — it doesn't ship a logging config.
Every Dewey log goes through a logger under the ``dewey.*`` namespace
(``dewey.core``, ``dewey.sqlalchemy.executor``, ``dewey.django.executor``,
etc.), so consumers can wire them up like any other library:

.. code-block:: python

    # Django LOGGING dict
    LOGGING = {
        "version": 1,
        "loggers": {"dewey": {"level": "INFO", "handlers": ["console"]}},
        ...
    }

    # stdlib
    logging.getLogger("dewey").setLevel("INFO")

Trace context
-------------

Dewey doesn't know what a "trace ID" means — it just round-trips a small
dict of correlation IDs from the task creator, through the
ledger row's ``metadata["trace"]`` field, into the worker, and makes that
dict available to every log record produced while the handler runs.

Producers (typically your API request handler) push values into the
context with :func:`set_trace_context` or merge them into ``metadata``
with :func:`bind_to_metadata` before calling ``create_task``. The
``process_*`` functions automatically restore the trace context from
``metadata["trace"]`` before invoking the handler and reset it after.

Consumers (anyone configuring logs) attach :class:`TraceContextFilter`
to their handler so every log record gains ``record.dewey_trace`` plus
flattened ``record.dewey_<field>`` attributes:

.. code-block:: python

    handler = logging.StreamHandler()
    handler.addFilter(TraceContextFilter())
    handler.setFormatter(JsonFormatter())  # python-json-logger, logfire, etc.

The carrier is intentionally library-agnostic. Put W3C ``traceparent``,
an OpenTelemetry context, an AWS X-Ray trace ID, a plain ``request_id``,
or all of the above into the dict. Dewey will round-trip it untouched.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any

#: Key inside a task's ``metadata`` dict used to carry
#: the trace context across the worker boundary.
TRACE_METADATA_KEY = "trace"
#: Key of the W3C trace parent inside the trace context.
TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"


# Default is ``None`` (not ``{}``) to avoid the well-known ContextVar
# mutable-default footgun; callers always go through get_trace_context()
# which normalizes to an empty dict.
_trace_context: ContextVar[dict[str, Any] | None] = ContextVar("dewey_trace_context", default=None)


def get_trace_context() -> dict[str, Any]:
    """Return a copy of the current trace context dict."""
    ctx = _trace_context.get()
    return dict(ctx) if ctx else {}


def set_trace_context(ctx: dict[str, Any] | None) -> Token[dict[str, Any] | None]:
    """Replace the trace context. Returns a token usable with :func:`reset_trace_context`.

    Pass ``None`` or ``{}`` to clear the context for the current frame.
    """
    return _trace_context.set(dict(ctx) if ctx else None)


def reset_trace_context(token: Token[dict[str, Any] | None]) -> None:
    """Restore the trace context to its previous value (paired with :func:`set_trace_context`)."""
    _trace_context.reset(token)


def update_trace_context(**fields: Any) -> Token[dict[str, Any] | None]:
    """Shallow-merge fields into the current trace context.

    Returns a token so the caller can restore the previous context.
    """
    current = get_trace_context()
    current.update(fields)
    return _trace_context.set(current)


def bind_to_metadata(
    metadata: dict[str, Any] | None = None,
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a ``metadata`` dict for ``create_task``
    that carries the current trace context.

    Merges, in order: ``metadata`` (caller-supplied), the current trace
    context (under ``"trace"``), and any ``extra`` fields the caller wants
    on top.

    .. code-block:: python

        await create_task_async(
            session,
            task_type="order.confirmed",
            payload=body,
            metadata=bind_to_metadata({"source": "webhook"}),
        )
    """
    out: dict[str, Any] = dict(metadata) if metadata else {}
    ctx = get_trace_context()
    for header, value in ((TRACEPARENT, current_traceparent()), (TRACESTATE, _tracestate())):
        if value is not None:
            ctx.setdefault(header, value)
    if ctx:
        existing = dict(out.get(TRACE_METADATA_KEY) or {})
        existing.update(ctx)
        out[TRACE_METADATA_KEY] = existing
    if extra:
        out.update(extra)
    return out


def extract_trace_context(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Pull the trace context dict out of a metadata blob (or ``{}``)."""
    if not metadata:
        return {}
    ctx = metadata.get(TRACE_METADATA_KEY)
    return dict(ctx) if isinstance(ctx, dict) else {}


@contextmanager
def restore_trace_context(metadata: dict[str, Any] | None) -> Iterator[dict[str, Any]]:
    """Context manager: temporarily set the trace context from a task or
    task's ``metadata`` blob, restoring the previous context on exit.

    Used by Dewey's ``process_*`` functions to make trace IDs that were
    captured at task-creation time available to logs produced inside the
    handler.
    """
    ctx = extract_trace_context(metadata)
    token = _trace_context.set(ctx)
    try:
        yield ctx
    finally:
        _trace_context.reset(token)


def current_traceparent() -> str | None:
    """The W3C ``traceparent`` of the active OpenTelemetry span, if there is one.

    OpenTelemetry is optional: without it (or without a recording span) task
    creation carries only what was put into the trace context by hand.
    """
    span = _active_span()
    if span is None:
        return None
    return f"00-{span.trace_id:032x}-{span.span_id:016x}-{int(span.trace_flags):02x}"


def _active_span() -> Any | None:
    try:
        from opentelemetry import trace  # type: ignore[import-not-found]
    except ImportError:
        return None
    span = trace.get_current_span().get_span_context()
    return span if span.is_valid else None


def _tracestate() -> str | None:
    """The vendor state that travels with a ``traceparent``, when there is any."""
    span = _active_span()
    return (span.trace_state.to_header() or None) if span is not None else None


@contextmanager
def restore_trace(trace: dict[str, Any] | None) -> Iterator[dict[str, Any]]:
    """Run a handler under the trace context its task was created with.

    Log records gain the creator's correlation fields, and when OpenTelemetry
    is installed and the task carries a ``traceparent``, spans started by the
    handler join the creator's trace.
    """
    ctx = dict(trace) if isinstance(trace, dict) else {}
    token = _trace_context.set(ctx or None)
    detach = None
    if isinstance(ctx.get(TRACEPARENT), str):
        try:
            from opentelemetry import context, propagate  # type: ignore[import-not-found]

            carrier = {h: ctx[h] for h in (TRACEPARENT, TRACESTATE) if isinstance(ctx.get(h), str)}
            attached = context.attach(propagate.extract(carrier))
            detach = lambda: context.detach(attached)  # noqa: E731
        except ImportError:
            pass
    try:
        yield ctx
    finally:
        if detach is not None:
            detach()
        _trace_context.reset(token)


class TraceContextFilter(logging.Filter):
    """Logging filter that attaches the current Dewey trace context to every record.

    Attach to a handler in your logging configuration; every record routed
    through that handler will gain:

    - ``record.dewey_trace`` — a ``dict`` of the full trace context
    - ``record.dewey_<field>`` — one attribute per key (e.g. ``record.dewey_request_id``)

    JSON formatters (logfire, python-json-logger, structlog renderers) pick
    these up automatically. Plain text formatters can reference them in the
    format string, e.g. ``"%(asctime)s [%(dewey_request_id)s] %(message)s"``.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = get_trace_context()
        record.dewey_trace = ctx  # type: ignore[attr-defined]
        for key, value in ctx.items():
            setattr(record, f"dewey_{key}", value)
        return True


__all__ = [
    "TRACEPARENT",
    "TRACE_METADATA_KEY",
    "TraceContextFilter",
    "bind_to_metadata",
    "current_traceparent",
    "extract_trace_context",
    "get_trace_context",
    "reset_trace_context",
    "restore_trace",
    "restore_trace_context",
    "set_trace_context",
    "update_trace_context",
]
