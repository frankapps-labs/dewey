"""Budgets and trace context through a real admitted execution on every storage."""

import asyncio
import sys
import time
from contextlib import nullcontext
from contextvars import ContextVar
from datetime import timedelta
from types import ModuleType, SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

import dewey
from dewey._worker.runtime import LedgerRuntime
from dewey.context import TaskContext, _bind_task
from dewey.core.logging import (
    bind_to_metadata,
    get_trace_context,
    reset_trace_context,
    restore_trace,
    set_trace_context,
)
from dewey.sqlalchemy import apply_task_timeouts, apply_task_timeouts_async
from tests.native.test_execution_budget import producer as producer
from tests.native.test_storage import snapshot

PARENT = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()


@pytest.fixture
def runtime(store):
    """A task process's runtime over the store; records every invocation it starts."""
    invocations = []

    def call(operation, *args):
        result = store(operation, *args)
        if operation == "start":
            invocations.append(result)
        return result

    storage = SimpleNamespace(call=call)
    if hasattr(store.storage, "handler_scope"):
        storage.handler_scope = store.storage.handler_scope
    runtime = LedgerRuntime(storage, close_resources=lambda: None)
    runtime.invocations = invocations
    yield runtime
    runtime.close()


def settings(connection) -> tuple[str, str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
        )
        return cursor.fetchone()


@pytest.fixture
def django_connection(store):
    """The Django worker's connection with the application's own timeouts set, or None."""
    if store.mode != "django":
        yield None
        return
    from django.db import connections

    connection = connections["native"]
    with connection.cursor() as cursor:
        cursor.execute("SET statement_timeout = '7s'")
        cursor.execute("SET lock_timeout = '3s'")
    yield connection
    with connection.cursor() as cursor:
        cursor.execute("RESET statement_timeout")
        cursor.execute("RESET lock_timeout")


class TestBudgets:
    @pytest.mark.parametrize("fails", [False, True])
    def test_the_handler_sees_its_attempts_deadline_and_database_budgets(
        self, store, producer, engine, runtime, django_connection, fails
    ):
        seen = []

        def handler():
            task = dewey.current_task()
            first = task.remaining()
            time.sleep(0.005)
            seen.append((task, first, task.remaining()))
            if django_connection is not None:  # applied to the handler's connection for it
                assert settings(django_connection) == ("1500ms", "250ms")
            if fails:
                raise ValueError("synthetic failure")

        dewey.task("work.budget", statement_timeout_s=1.5, lock_timeout_s=0.25)(handler)
        task_id = producer(task_type="work.budget", execution_timeout_s=10)
        (assignment,) = store("claim", 1)
        deadline = snapshot(engine, task_id)["execution_deadline_at"]
        runtime.execute(assignment)
        ((task, first, later),) = seen
        assert task.deadline == deadline and task.deadline.utcoffset() == timedelta(0)
        assert timedelta(0) < later < first <= timedelta(seconds=10)
        assert (task.statement_timeout_s, task.lock_timeout_s) == (1.5, 0.25)
        assert snapshot(engine, task_id)["status"] == ("failed" if fails else "completed")
        with pytest.raises(RuntimeError):
            dewey.current_task()  # the handler's identity ends with it
        if django_connection is not None:  # and the application's own settings are back
            assert settings(django_connection) == ("7s", "3s")

    @pytest.mark.parametrize("store", ["django"], indirect=True)
    @pytest.mark.parametrize("in_transaction", [False, True])
    def test_django_stops_a_statement_over_budget_and_restores_the_connection(
        self, store, producer, engine, runtime, django_connection, in_transaction
    ):
        from django.db import DatabaseError, transaction

        errors = []

        def handler():
            try:
                with transaction.atomic(using="native") if in_transaction else nullcontext():
                    with django_connection.cursor() as cursor:
                        cursor.execute("SELECT pg_sleep(1)")
            except DatabaseError as exc:
                errors.append(str(exc))
                raise

        dewey.task("work.slow", statement_timeout_s=0.01)(handler)
        task_id = producer(task_type="work.slow")
        runtime.execute(store("claim", 1)[0])
        row = snapshot(engine, task_id)
        assert (row["status"], row["latest_outcome"]) == ("failed", "handler_failure")
        assert len(errors) == 1 and "statement timeout" in errors[0]
        assert settings(django_connection) == ("7s", "3s")

    def test_sqlalchemy_budget_stops_a_slow_statement_and_ends_with_its_transaction(self, engine):
        task = TaskContext("task-1", "work.slow", 1, statement_timeout_s=0.01)
        with engine.connect() as connection:
            with (
                pytest.raises(DBAPIError, match="statement timeout"),
                connection.begin(),
                _bind_task(task),
            ):
                apply_task_timeouts(connection)
                connection.execute(text("SELECT pg_sleep(1)"))
            assert connection.execute(text("SHOW statement_timeout")).scalar_one() == "0"

    @pytest.mark.asyncio
    async def test_async_sqlalchemy_budget_does_the_same(self, async_engine):
        task = TaskContext("task-1", "work.slow", 1, statement_timeout_s=0.01)
        async with async_engine.connect() as connection:
            with pytest.raises(DBAPIError, match="statement timeout"):
                async with connection.begin():
                    with _bind_task(task):
                        await apply_task_timeouts_async(connection)
                    await connection.execute(text("SELECT pg_sleep(1)"))
            shown = await connection.execute(text("SHOW statement_timeout"))
            assert shown.scalar_one() == "0"


class TestTrace:
    @pytest.mark.parametrize("idempotent", [False, True])
    @pytest.mark.parametrize("fails", [False, True])
    def test_the_creators_trace_reaches_the_handler_and_ends_with_it(
        self, store, producer, engine, runtime, idempotent, fails
    ):
        expected = {"upstream": "edge", "request_id": "req-1", "traceparent": PARENT}
        metadata = {"trace": {"upstream": "edge"}, "private": "kept-out"}
        token = set_trace_context({"request_id": "req-1", "traceparent": PARENT})
        try:
            task_id = producer(
                task_type="work.traced",
                metadata=metadata,
                **({"idempotency_key": "traced-once"} if idempotent else {}),
            )
        finally:
            reset_trace_context(token)
        assert snapshot(engine, task_id)["metadata"]["trace"] == expected
        assert metadata == {"trace": {"upstream": "edge"}, "private": "kept-out"}  # not mutated
        seen = []

        def handler():
            seen.append(get_trace_context())
            if fails:
                raise ValueError("synthetic failure")

        async def async_handler():
            await asyncio.sleep(0)
            handler()

        dewey.task("work.traced")(async_handler if store.mode == "async" else handler)
        outer = set_trace_context({"outer": "caller"})
        try:
            runtime.execute(store("claim", 1)[0])
            assert get_trace_context() == {"outer": "caller"}  # even when the handler raised
        finally:
            reset_trace_context(outer)
        (invocation,) = runtime.invocations
        assert seen == [expected] and invocation["trace"] == expected
        assert "kept-out" not in repr(invocation)  # only the trace travels, not the metadata
        assert snapshot(engine, task_id)["status"] == ("failed" if fails else "completed")


@pytest.fixture
def tracing(monkeypatch):
    """A stand-in for the optional tracing library: just the calls Dewey makes."""
    active = ContextVar("active_span", default=None)

    def span(traceparent: str, tracestate: str = ""):
        _, trace_id, span_id, flags = traceparent.split("-")
        return SimpleNamespace(
            is_valid=True,
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            trace_flags=int(flags, 16),
            trace_state=SimpleNamespace(to_header=lambda: tracestate),
        )

    package = ModuleType("opentelemetry")
    package.trace = ModuleType("opentelemetry.trace")
    package.context = ModuleType("opentelemetry.context")
    package.propagate = ModuleType("opentelemetry.propagate")
    package.trace.get_current_span = lambda: SimpleNamespace(
        get_span_context=lambda: active.get() or SimpleNamespace(is_valid=False)
    )
    package.propagate.extract = lambda carrier: span(
        carrier["traceparent"], carrier.get("tracestate", "")
    )
    package.context.attach = active.set
    package.context.detach = active.reset
    for module in (package, package.trace, package.context, package.propagate):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    return SimpleNamespace(active=active, span=span)


class TestOptionalTracingLibrary:
    def test_the_active_span_is_captured_at_creation_and_joined_by_the_handler(
        self, store, producer, engine, runtime, tracing
    ):
        creating = tracing.active.set(tracing.span(PARENT))
        try:
            task_id = producer(task_type="work.traced")
        finally:
            tracing.active.reset(creating)
        assert snapshot(engine, task_id)["metadata"]["trace"] == {"traceparent": PARENT}
        seen = []
        dewey.task("work.traced")(lambda: seen.append(tracing.active.get()))
        runtime.execute(store("claim", 1)[0])
        assert [f"{span.trace_id:032x}" for span in seen] == [PARENT.split("-")[1]]
        assert tracing.active.get() is None  # detached once the handler returned
        assert snapshot(engine, task_id)["status"] == "completed"

    def test_vendor_state_travels_with_the_trace_parent(
        self, store, producer, engine, runtime, tracing
    ):
        creating = tracing.active.set(tracing.span(PARENT, "vendor=abc"))
        try:
            task_id = producer(task_type="work.traced")
        finally:
            tracing.active.reset(creating)
        carried = {"traceparent": PARENT, "tracestate": "vendor=abc"}
        assert snapshot(engine, task_id)["metadata"]["trace"] == carried
        seen = []
        dewey.task("work.traced")(lambda: seen.append(tracing.active.get().trace_state.to_header()))
        runtime.execute(store("claim", 1)[0])
        assert seen == ["vendor=abc"]

    def test_a_trace_parent_set_by_hand_wins_over_the_active_span(self, tracing):
        creating = tracing.active.set(tracing.span("00-" + "f" * 32 + "-" + "e" * 16 + "-01"))
        token = set_trace_context({"traceparent": PARENT})
        try:
            assert bind_to_metadata() == {"trace": {"traceparent": PARENT}}
        finally:
            reset_trace_context(token)
            tracing.active.reset(creating)

    def test_the_span_is_detached_when_the_handler_raises(self, tracing):
        with pytest.raises(ValueError), restore_trace({"traceparent": PARENT}):
            assert tracing.active.get().span_id == int(PARENT.split("-")[2], 16)
            raise ValueError("synthetic failure")
        assert tracing.active.get() is None and get_trace_context() == {}

    def test_without_the_library_the_plain_context_still_travels(self, monkeypatch):
        for name in ("opentelemetry", "opentelemetry.trace"):
            monkeypatch.setitem(sys.modules, name, None)  # importing it now fails
        token = set_trace_context({"traceparent": PARENT})
        try:
            assert bind_to_metadata() == {"trace": {"traceparent": PARENT}}
            with restore_trace({"request_id": "req-1", "traceparent": PARENT}) as restored:
                assert get_trace_context() == restored
            assert get_trace_context() == {"traceparent": PARENT}
        finally:
            reset_trace_context(token)
