"""Tests for the trace-context propagation and logging filter."""

from __future__ import annotations

import asyncio
import logging

import pytest

from dewey.core.logging import (
    TRACE_METADATA_KEY,
    TraceContextFilter,
    bind_to_metadata,
    extract_trace_context,
    get_trace_context,
    reset_trace_context,
    restore_trace_context,
    set_trace_context,
    update_trace_context,
)
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.async_notifications import (
    create_notification_async,
    send_notification_async,
)
from dewey.sqlalchemy.executor import create_task, process_task

# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


class TestPrimitives:
    def test_default_context_is_empty(self):
        assert get_trace_context() == {}

    def test_set_and_reset(self):
        token = set_trace_context({"request_id": "abc"})
        assert get_trace_context() == {"request_id": "abc"}
        reset_trace_context(token)
        assert get_trace_context() == {}

    def test_set_returns_copy(self):
        src = {"request_id": "abc"}
        token = set_trace_context(src)
        src["request_id"] = "mutated"
        assert get_trace_context() == {"request_id": "abc"}
        reset_trace_context(token)

    def test_get_returns_copy(self):
        token = set_trace_context({"request_id": "abc"})
        snapshot = get_trace_context()
        snapshot["request_id"] = "mutated"
        assert get_trace_context() == {"request_id": "abc"}
        reset_trace_context(token)

    def test_update_merges(self):
        token1 = set_trace_context({"request_id": "abc"})
        token2 = update_trace_context(user_id="u1")
        assert get_trace_context() == {"request_id": "abc", "user_id": "u1"}
        reset_trace_context(token2)
        assert get_trace_context() == {"request_id": "abc"}
        reset_trace_context(token1)

    def test_set_none_clears(self):
        token = set_trace_context({"x": "y"})
        token2 = set_trace_context(None)
        assert get_trace_context() == {}
        reset_trace_context(token2)
        reset_trace_context(token)

    def test_bind_to_metadata_with_no_ctx(self):
        # No active context — metadata passes through.
        assert bind_to_metadata() == {}
        assert bind_to_metadata({"source": "x"}) == {"source": "x"}

    def test_bind_to_metadata_merges_context(self):
        token = set_trace_context({"request_id": "abc"})
        out = bind_to_metadata({"source": "webhook"}, extra={"version": 2})
        assert out == {
            "source": "webhook",
            "trace": {"request_id": "abc"},
            "version": 2,
        }
        reset_trace_context(token)

    def test_bind_preserves_existing_trace_in_metadata(self):
        token = set_trace_context({"request_id": "abc"})
        out = bind_to_metadata({"trace": {"upstream_id": "x"}})
        assert out["trace"] == {"upstream_id": "x", "request_id": "abc"}
        reset_trace_context(token)

    def test_extract_trace_context(self):
        assert extract_trace_context(None) == {}
        assert extract_trace_context({}) == {}
        assert extract_trace_context({"trace": {"r": "1"}}) == {"r": "1"}
        # Defensive: non-dict trace value is ignored.
        assert extract_trace_context({"trace": "oops"}) == {}

    def test_restore_trace_context_cm(self):
        outer_token = set_trace_context({"outer": "1"})
        with restore_trace_context({TRACE_METADATA_KEY: {"inner": "2"}}) as inner:
            assert inner == {"inner": "2"}
            assert get_trace_context() == {"inner": "2"}
        assert get_trace_context() == {"outer": "1"}
        reset_trace_context(outer_token)


# ---------------------------------------------------------------------------
# Logging filter
# ---------------------------------------------------------------------------


class TestLoggingFilter:
    def test_filter_adds_attrs(self):
        token = set_trace_context({"request_id": "abc", "user_id": "u1"})
        record = logging.LogRecord(
            name="dewey.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hi",
            args=(),
            exc_info=None,
        )
        f = TraceContextFilter()
        assert f.filter(record) is True
        assert record.dewey_trace == {"request_id": "abc", "user_id": "u1"}  # type: ignore[attr-defined]
        assert record.dewey_request_id == "abc"  # type: ignore[attr-defined]
        assert record.dewey_user_id == "u1"  # type: ignore[attr-defined]
        reset_trace_context(token)

    def test_filter_with_no_context(self):
        record = logging.LogRecord(
            name="dewey.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hi",
            args=(),
            exc_info=None,
        )
        f = TraceContextFilter()
        assert f.filter(record) is True
        assert record.dewey_trace == {}  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Sync executor: trace restored inside handler
# ---------------------------------------------------------------------------


class TestSyncExecutorTrace:
    def test_handler_sees_trace_from_metadata(self, session):
        seen: dict[str, dict] = {}

        def handler(task_type, payload):
            seen["ctx"] = get_trace_context()

        task = create_task(session, task_type="t", metadata={"trace": {"request_id": "REQ-1"}})
        session.commit()
        process_task(session, task.id, handler)

        assert seen["ctx"] == {"request_id": "REQ-1"}
        # Context cleared again after handler returns.
        assert get_trace_context() == {}

    def test_handler_no_trace_when_metadata_empty(self, session):
        seen: dict[str, dict] = {}

        def handler(task_type, payload):
            seen["ctx"] = get_trace_context()

        task = create_task(session, task_type="t")
        session.commit()
        process_task(session, task.id, handler)
        assert seen["ctx"] == {}


# ---------------------------------------------------------------------------
# Async executor + notification: trace restored
# ---------------------------------------------------------------------------


class TestAsyncTrace:
    @pytest.mark.asyncio
    async def test_async_handler_sees_trace(self, async_session):
        seen: dict[str, dict] = {}

        async def handler(task_type, payload):
            seen["ctx"] = get_trace_context()

        task = await create_task_async(
            async_session, task_type="t", metadata={"trace": {"request_id": "REQ-A"}}
        )
        await async_session.commit()
        await process_task_async(async_session, task.id, handler)
        assert seen["ctx"] == {"request_id": "REQ-A"}
        assert get_trace_context() == {}

    @pytest.mark.asyncio
    async def test_concurrent_tasks_do_not_bleed_context(self, async_session):
        # Two tasks with different trace ids, run "concurrently" (sequentially in this
        # single-session test but interleaved via asyncio.gather and small sleeps).
        seen: dict[str, dict] = {}

        async def handler_a(task_type, payload):
            await asyncio.sleep(0.05)
            seen["a"] = get_trace_context()

        async def handler_b(task_type, payload):
            await asyncio.sleep(0.05)
            seen["b"] = get_trace_context()

        # Two separate sessions to allow real concurrency.
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(async_session.bind, expire_on_commit=False)

        async def run(metadata, handler):
            async with factory() as s:
                t = await create_task_async(s, task_type="t", metadata=metadata)
                await s.commit()
            async with factory() as s:
                await process_task_async(s, t.id, handler)

        await asyncio.gather(
            run({"trace": {"id": "A"}}, handler_a),
            run({"trace": {"id": "B"}}, handler_b),
        )
        assert seen["a"] == {"id": "A"}
        assert seen["b"] == {"id": "B"}

    @pytest.mark.asyncio
    async def test_trace_covers_phase_3_logs(self, async_session, caplog):
        """Regression: Dewey's own status logs (Task completed, Task failed) must
        be emitted while the trace context is still active — not just the
        handler invocation."""
        caplog.set_level(logging.INFO, logger="dewey.sqlalchemy.async_executor")
        f = TraceContextFilter()
        for h in caplog.handler, *logging.getLogger().handlers:
            h.addFilter(f)
        try:

            async def handler(task_type, payload):
                return None  # success path → Phase 3b runs

            task = await create_task_async(
                async_session,
                task_type="t",
                metadata={"trace": {"request_id": "REQ-PHASE3"}},
            )
            await async_session.commit()
            await process_task_async(async_session, task.id, handler)
        finally:
            for h in caplog.handler, *logging.getLogger().handlers:
                h.removeFilter(f)

        completed = [r for r in caplog.records if "Task completed" in r.getMessage()]
        assert completed, "expected a 'Task completed' log record"
        for r in completed:
            assert getattr(r, "dewey_request_id", None) == "REQ-PHASE3"

    @pytest.mark.asyncio
    async def test_send_notification_restores_trace(self, async_session):
        seen: dict[str, dict] = {}

        class CapturingChannel:
            name = "cap"

            def send(self, recipient, subject, body, payload):
                from dewey.core.notifications import ChannelResult

                seen["ctx"] = get_trace_context()
                return ChannelResult(success=True)

        notif = await create_notification_async(
            async_session,
            event_type="evt",
            channel="cap",
            recipient="x@y.z",
            metadata={"trace": {"request_id": "REQ-N"}},
        )
        await async_session.commit()
        await send_notification_async(async_session, notif.id, CapturingChannel())
        assert seen["ctx"] == {"request_id": "REQ-N"}
        assert get_trace_context() == {}
