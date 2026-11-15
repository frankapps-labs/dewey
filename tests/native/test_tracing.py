"""An OpenTelemetry span per attempt, in the creator's trace, through the real SDK.

The API is optional and the feature is opt-in; these tests install the SDK's
in-memory exporter once for the session (``tests._helpers.tracing``) and prove,
on every storage: the creator's span is the attempt span's parent, the handler's
own spans nest under it, a raised exception is recorded with error status, a
schedule tick starts a trace of its own, and task processes started by spawn or
fork export their spans and flush them when they stop.
"""

import asyncio
import importlib
import json
import logging
import sys
import time
from types import SimpleNamespace

import pytest
from opentelemetry import trace

import dewey
from dewey._worker.pool import WorkerPool
from dewey._worker.runner import NativeRunner
from dewey._worker.runtime import LedgerRuntime
from dewey.core.logging import get_trace_context, set_trace_context
from dewey.core.tracing import configure_tracing, tracing_enabled
from dewey.worker import default_timing
from tests._helpers import native_runtime
from tests._helpers.tracing import EXPORTER, describe, install_provider
from tests.native.test_execution_budget import producer as producer
from tests.native.test_runner import StorageBridge, drive
from tests.native.test_storage import create, snapshot


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()


@pytest.fixture
def tracing():
    install_provider()
    EXPORTER.clear()
    configure_tracing(True)
    yield SimpleNamespace(tracer=trace.get_tracer("tests"))
    configure_tracing(False)
    EXPORTER.clear()


@pytest.fixture
def runtime(store):
    storage = SimpleNamespace(call=store)
    if hasattr(store.storage, "handler_scope"):
        storage.handler_scope = store.storage.handler_scope
    runtime = LedgerRuntime(storage, close_resources=lambda: None)
    yield runtime
    runtime.close()


def spans():
    return {span.name: describe(span) for span in EXPORTER.get_finished_spans()}


def register(store, name, body, **policy):
    def handler():
        body()

    async def async_handler():
        await asyncio.sleep(0)
        body()

    dewey.task(name, **policy)(async_handler if store.mode == "async" else handler)


@pytest.mark.parametrize("fails", [False, True])
def test_the_attempt_is_a_span_in_the_creators_trace(
    store, producer, engine, runtime, tracing, fails
):
    with tracing.tracer.start_as_current_span("POST /photos"):
        task_id = producer(task_type="photos.moderate", max_attempts=3)
    assert "traceparent" in snapshot(engine, task_id)["metadata"]["trace"]

    def body():
        with trace.get_tracer("app").start_as_current_span("resize"):
            pass
        if fails:
            raise ValueError("synthetic moderation failure")

    register(store, "photos.moderate", body)
    runtime.execute(store("claim", 1)[0])
    assert trace.get_current_span() is trace.INVALID_SPAN  # nothing leaks past the attempt

    seen = spans()
    request, attempt, resize = (
        seen[n] for n in ("POST /photos", "dewey task photos.moderate", "resize")
    )
    assert attempt["trace_id"] == request["trace_id"]
    assert attempt["parent_span_id"] == request["span_id"]
    assert attempt["kind"] == "CONSUMER"
    assert attempt["attributes"] == {
        "messaging.system": "dewey",
        "messaging.operation.type": "process",
        "messaging.destination.name": "default",
        "messaging.message.id": task_id,
        "dewey.task.id": task_id,
        "dewey.task.type": "photos.moderate",
        "dewey.task.queue": "default",
        "dewey.task.attempt": 1,
        "dewey.task.max_attempts": 3,
    }
    assert resize["parent_span_id"] == attempt["span_id"]  # the handler's spans nest
    if fails:
        assert attempt["status"] == "ERROR"
        assert attempt["description"] == "ValueError: synthetic moderation failure"
        assert attempt["exceptions"] == [
            {"type": "ValueError", "message": "synthetic moderation failure"}
        ]
        assert snapshot(engine, task_id)["status"] == "failed"
    else:
        assert attempt["status"] == "UNSET" and attempt["exceptions"] == []
        assert snapshot(engine, task_id)["status"] == "completed"


def test_a_task_without_a_creator_trace_starts_its_own(store, engine, runtime, tracing):
    """A schedule tick, or a task created where nothing was traced, is a root span."""
    task_id = create(engine, task_type="reports.nightly", task_metadata={"schedule": "nightly"})
    register(store, "reports.nightly", lambda: None)
    runtime.execute(store("claim", 1)[0])
    attempt = spans()["dewey task reports.nightly"]
    assert attempt["parent_span_id"] is None
    assert attempt["attributes"]["dewey.task.id"] == task_id


def test_tracing_is_off_unless_asked_for(store, producer, runtime):
    install_provider()
    EXPORTER.clear()
    assert not tracing_enabled()
    with trace.get_tracer("tests").start_as_current_span("POST /photos"):
        producer(task_type="photos.moderate")
    register(store, "photos.moderate", lambda: None)
    runtime.execute(store("claim", 1)[0])
    assert [name for name in spans() if name.startswith("dewey task")] == []


def test_turning_tracing_on_without_the_library_fails_at_startup(monkeypatch):
    for name in list(sys.modules):
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(RuntimeError, match=r"dewey\[otel\]"):
        configure_tracing(True)
    assert not tracing_enabled()
    configure_tracing(False)


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_task_processes_export_their_spans_and_flush_on_stop(
    store, producer, engine, tmp_path, monkeypatch, tracing, start_method
):
    """The span is made in the task process. Its batch exporter would only send
    after a minute; the process flushes when the worker stops it, so the spans
    of a short-lived worker are not lost. With the worker's default timing the
    pool stops an idle child after termination_seconds / 4 (1 s): the export
    here takes longer than that, and still gets through, because idle children
    share the whole termination budget to close."""
    if "native.synthetic.fail" not in dewey.registry.task_types():
        importlib.reload(native_runtime)
    if store.mode == "django" and start_method == "fork":
        monkeypatch.setenv("DEWEY_NATIVE_TEST_ALIAS", "native")  # the parent's alias
    exported = tmp_path / "spans.jsonl"
    monkeypatch.setenv("DEWEY_NATIVE_TEST_SPANS", str(exported))
    monkeypatch.setenv("DEWEY_NATIVE_TEST_MODE", store.mode)
    monkeypatch.setenv("DEWEY_NATIVE_TEST_OWNER", store.owner)
    with tracing.tracer.start_as_current_span("POST /photos"):
        task_id = producer(task_type="native.synthetic.fail", max_attempts=1)
    request = spans()["POST /photos"]
    monkeypatch.setenv("DEWEY_NATIVE_TEST_SPAN_EXPORT_SECONDS", "1.5")  # slower than 4 s / 4
    timing = default_timing(1)  # the worker's own defaults: termination_seconds=4
    pool = WorkerPool(
        "tests._helpers.native_runtime:create_runtime",
        concurrency=1,
        task_timeout_seconds=timing.task_timeout_seconds,
        termination_timeout_seconds=timing.termination_seconds / 4,  # as service() does
        cleanup_timeout_seconds=timing.termination_seconds,
        start_method=start_method,
    )
    runner = NativeRunner(
        pool, StorageBridge(store), timing=timing, close_resources=store.close_resources
    )
    try:
        runner.start()
        pids = set(pool.worker_pids)
        drive(runner, engine, task_id, "dead")
        until = time.monotonic() + 5
        while runner.assignments and time.monotonic() < until:
            runner.step()
            time.sleep(0.005)
        assert not exported.exists() or exported.read_text() == ""  # not sent yet
    finally:
        runner.close()
    records = [json.loads(line) for line in exported.read_text().splitlines()]
    (attempt,) = [r for r in records if r["attributes"].get("dewey.task.id") == task_id]
    assert attempt["pid"] in pids
    assert attempt["name"] == "dewey task native.synthetic.fail"
    assert attempt["trace_id"] == request["trace_id"]
    assert attempt["parent_span_id"] == request["span_id"]
    assert attempt["status"] == "ERROR"
    assert attempt["exceptions"] == [{"type": "ValueError", "message": "synthetic handler failure"}]
    assert attempt["attributes"]["dewey.task.attempt"] == 1


class TestFailedAttemptTelemetryIsImmuneToTheAlertHook:
    """The failed attempt's span and its log record arrive whatever the alert hook does.

    Logfire's handler sends a record to its fallback instead of Logfire, and its
    span processor drops a span at end, when OpenTelemetry's instrumentation-
    suppression flag is set in the current context. The alert hook now runs
    after the record and in a copy of the context, so a hook that leaks such a
    flag (an instrumented HTTP call, a forgotten span) cannot silence the
    attempt's own telemetry.
    """

    SUPPRESS = "suppress_instrumentation"  # the key Logfire and OTel instrumentations honour

    @pytest.fixture
    def suppression_aware_log(self):
        """A handler with Logfire's rule: a record under suppression never reaches the backend."""
        from opentelemetry import context

        records = []

        class Backend(logging.Handler):
            def emit(self, record):
                if context.get_value(TestFailedAttemptTelemetryIsImmuneToTheAlertHook.SUPPRESS):
                    return  # Logfire would hand it to its stderr fallback instead
                records.append((record, trace.get_current_span().get_span_context().span_id))

        handler = Backend()
        logging.getLogger("dewey.worker").addHandler(handler)
        yield records
        logging.getLogger("dewey.worker").removeHandler(handler)

    @pytest.fixture
    def leaking_hook(self):
        """An alert hook that leaves the context dirty: a suppression flag and a trace field."""
        from opentelemetry import context

        calls = []

        def hook(event):
            calls.append((event["event"], trace.get_current_span().get_span_context().span_id))
            context.attach(context.set_value(self.SUPPRESS, True))  # never detached
            set_trace_context({"leaked": "yes"})

        dewey.on_alert(hook)
        yield calls
        dewey.on_alert(None)

    def test_span_and_record_arrive_once_and_the_hook_leaves_no_trace(
        self, store, producer, engine, runtime, tracing, suppression_aware_log, leaking_hook
    ):
        from opentelemetry import context

        def fail():
            raise ValueError("synthetic moderation failure")

        register(store, "photos.moderate", fail, max_attempts=1)
        task_id = producer(task_type="photos.moderate")
        runtime.execute(store("claim", 1)[0])
        assert snapshot(engine, task_id)["status"] == "dead"

        attempt = spans()["dewey task photos.moderate"]
        assert attempt["status"] == "ERROR" and len(attempt["exceptions"]) == 1
        ((record, span_id),) = suppression_aware_log  # once, and not under suppression
        assert record.levelno == logging.ERROR and record.task_id == task_id
        assert f"{span_id:016x}" == attempt["span_id"]  # inside the attempt span
        ((event, hook_span),) = leaking_hook
        assert event == "task_dead" and f"{hook_span:016x}" == attempt["span_id"]  # it sees it
        assert context.get_value(self.SUPPRESS) is None  # nothing the hook attached survives
        assert get_trace_context() == {}
