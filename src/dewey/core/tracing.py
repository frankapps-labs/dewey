"""One OpenTelemetry span per attempt, through the API only, when asked for.

Dewey never depends on OpenTelemetry. With ``opentelemetry-api`` importable
(``pip install dewey[otel]``) and tracing turned on in the worker settings
(``tracing: true``, Django ``DEWEY["TRACING"] = True``), each attempt a task
process runs is a span named ``dewey task <type>``:

- a child of the span that created the task, when the row carries the creator's
  ``traceparent`` (every ``create_task`` stores it); otherwise the root of a new
  trace, which is what a schedule tick gets;
- of kind ``CONSUMER`` with ``messaging.system = "dewey"`` plus the task's
  identity as attributes (``dewey.task.id``, ``.type``, ``.queue``, ``.attempt``,
  ``.max_attempts``);
- the current span while the handler runs, so spans the handler starts nest
  under it;
- ended with the exception recorded and the status set to error when the
  handler raised.

The SDK, or a vendor distribution such as Logfire, exports the spans: the
application configures it in the worker process (the factory, or Django's
``settings``) exactly as for its web process. A task process flushes the
provider when it stops, because multiprocessing children run no ``atexit``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

TRACER_NAME = "dewey"
INSTALL_HINT = "tracing needs opentelemetry-api: pip install 'dewey[otel]'"

_enabled = False


def configure_tracing(enabled: bool) -> None:
    """Turn attempt spans on or off for this process.

    Raises ``RuntimeError`` when turned on without ``opentelemetry-api``: a
    worker configured to trace must not run silently untraced.
    """
    global _enabled
    if enabled and _otel() is None:
        raise RuntimeError(INSTALL_HINT)
    _enabled = bool(enabled)


def tracing_enabled() -> bool:
    return _enabled


def _otel() -> Any | None:
    try:
        from opentelemetry import context, trace  # type: ignore[import-not-found]
        from opentelemetry.trace import SpanKind, StatusCode  # type: ignore[import-not-found]
    except ImportError:
        return None
    return context, trace, SpanKind, StatusCode


class AttemptSpan:
    """What the runtime holds while an attempt runs: a span, or nothing."""

    def __init__(self, span: Any | None, status_code: Any | None) -> None:
        self._span = span
        self._error = status_code

    def failed(self, exc: BaseException) -> None:
        """Record the handler's exception and mark the attempt as an error."""
        if self._span is None:
            return
        self._span.record_exception(exc)
        self._span.set_status(self._error, f"{type(exc).__name__}: {exc}")

    def set(self, name: str, value: Any) -> None:
        if self._span is not None:
            self._span.set_attribute(name, value)


_NONE = AttemptSpan(None, None)


@contextmanager
def attempt_span(task_id: str, invocation: dict[str, Any]) -> Iterator[AttemptSpan]:
    """Run one attempt under its span; a no-op unless tracing is on.

    Enter it after the creator's trace context has been attached (see
    ``restore_trace``) so the span becomes that trace's child. The span covers
    the handler and the outcome write that follows it.
    """
    otel = _otel() if _enabled else None
    if otel is None:
        yield _NONE
        return
    context, trace, SpanKind, StatusCode = otel
    task_type = invocation["task_type"]
    attributes = {
        "messaging.system": "dewey",
        "messaging.operation.type": "process",
        "messaging.destination.name": invocation.get("queue", ""),
        "messaging.message.id": task_id,
        "dewey.task.id": task_id,
        "dewey.task.type": task_type,
        "dewey.task.queue": invocation.get("queue", ""),
        "dewey.task.attempt": invocation["attempts"],
        "dewey.task.max_attempts": invocation.get("max_attempts", 0),
    }
    tracer = trace.get_tracer(TRACER_NAME)
    span = tracer.start_span(
        f"dewey task {task_type}", kind=SpanKind.CONSUMER, attributes=attributes
    )
    token = context.attach(trace.set_span_in_context(span))
    try:
        yield AttemptSpan(span, StatusCode.ERROR)
    finally:
        context.detach(token)
        span.end()


DEFAULT_FLUSH_SECONDS = 5.0


def flush_spans(timeout_s: float | None = None) -> None:
    """Export what the provider still holds; call before a task process exits.

    The API's own provider has nothing to flush; the SDK's (and a vendor's)
    does. A task process is a multiprocessing child, which runs no ``atexit``
    hooks, so the batch exporter's buffer would otherwise be lost on a stop.
    ``timeout_s`` is what the caller can afford: the worker passes the time it
    gives a task process to close (``termination_seconds``), so a flush never
    outlives the process.
    """
    otel = _otel() if _enabled else None
    if otel is None:
        return
    provider = otel[1].get_tracer_provider()
    force_flush = getattr(provider, "force_flush", None)
    if callable(force_flush):
        seconds = DEFAULT_FLUSH_SECONDS if timeout_s is None else timeout_s
        force_flush(int(seconds * 1000))


__all__ = [
    "AttemptSpan",
    "attempt_span",
    "configure_tracing",
    "flush_spans",
    "tracing_enabled",
]
