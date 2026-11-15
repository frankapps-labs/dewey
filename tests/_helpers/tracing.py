"""One real OpenTelemetry SDK provider for the test session.

The API allows one global provider per process, so every tracing test shares
this one: an in-memory exporter for in-process assertions, and a batch exporter
that writes JSON lines to the file named by ``DEWEY_NATIVE_TEST_SPANS`` for the
spans a task process makes. Its long schedule delay means a span reaches the
file only when the process flushes on shutdown, which is the point.
"""

from __future__ import annotations

import json
import os
import time

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

PROVIDER: TracerProvider | None = None
EXPORTER = InMemorySpanExporter()


def describe(span) -> dict:
    """The parts of a finished span a test asserts on, JSON-safe."""
    context = span.get_span_context()
    parent = span.parent
    return {
        "name": span.name,
        "kind": span.kind.name,
        "trace_id": f"{context.trace_id:032x}",
        "span_id": f"{context.span_id:016x}",
        "parent_span_id": None if parent is None else f"{parent.span_id:016x}",
        "status": span.status.status_code.name,
        "description": span.status.description,
        "attributes": dict(span.attributes or {}),
        "exceptions": [
            {
                "type": event.attributes.get("exception.type"),
                "message": event.attributes.get("exception.message"),
            }
            for event in span.events
            if event.name == "exception"
        ],
        "pid": os.getpid(),
    }


class _FileExporter(SpanExporter):
    def export(self, spans) -> SpanExportResult:
        path = os.environ.get("DEWEY_NATIVE_TEST_SPANS")
        if path is not None:
            # A slow backend: the flush at shutdown has to be given this long.
            time.sleep(float(os.environ.get("DEWEY_NATIVE_TEST_SPAN_EXPORT_SECONDS", "0")))
            with open(path, "a") as output:
                for span in spans:
                    output.write(json.dumps(describe(span)) + "\n")
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


def install_provider() -> TracerProvider:
    """Set the session's provider once; later calls (and forked children) reuse it."""
    global PROVIDER
    if PROVIDER is None:
        PROVIDER = TracerProvider()
        PROVIDER.add_span_processor(SimpleSpanProcessor(EXPORTER))
        PROVIDER.add_span_processor(
            BatchSpanProcessor(_FileExporter(), schedule_delay_millis=60_000)
        )
        trace.set_tracer_provider(PROVIDER)
    return PROVIDER
