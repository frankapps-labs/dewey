"""A worker needs only a factory: defaults, the runtime's handler context, alerts."""

import time
from datetime import UTC, datetime, timedelta

import pytest

import dewey
from dewey import alerts
from dewey._worker.contracts import round_budget
from dewey._worker.pool import Assignment
from dewey._worker.runtime import LedgerRuntime
from dewey.core.logging import get_trace_context
from dewey.worker import WorkerConfig, default_timing


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()
    alerts.on_alert(None)


class TestConfiguration:
    def test_only_the_factory_is_required(self):
        config = WorkerConfig.from_dict({"factory": "app.worker:create"})
        assert (config.queues, config.concurrency) == (("default",), 2)
        assert config.auto_apply_schedules is False and config.alert_hook is None
        assert config.timing == default_timing(2)
        assert WorkerConfig.from_dict(config.to_dict()) == config

    @pytest.mark.parametrize("concurrency", [1, 2, 8, 32])
    def test_default_leases_fit_a_worst_case_round(self, concurrency):
        timing = default_timing(concurrency)
        needed = timing.renewal_interval_seconds + round_budget(timing, concurrency)
        assert needed < timing.execution_lease_seconds == timing.assignment_lease_seconds
        WorkerConfig(factory="app.worker:create", concurrency=concurrency)  # accepted

    def test_named_limits_override_and_leases_follow(self):
        config = WorkerConfig.from_dict(
            {"factory": "app.worker:create", "timing": {"task_timeout_seconds": 600}}
        )
        assert config.timing.task_timeout_seconds == 600
        assert config.timing.execution_lease_seconds == default_timing(2).execution_lease_seconds
        slow = default_timing(2, db_wait_seconds=10)
        assert slow.execution_lease_seconds > default_timing(2).execution_lease_seconds
        assert default_timing(2, execution_lease_seconds=500).execution_lease_seconds == 500

    @pytest.mark.parametrize(
        "settings",
        [
            {},
            {"factory": "app.worker:create", "timing": {"patience": 1}},
            {"factory": "app.worker:create", "alert_hook": "no-colon"},
            {"factory": "app.worker:create", "auto_apply_schedules": "yes"},
            {"factory": "app.worker:create", "database_password": "x"},
        ],
    )
    def test_unknown_or_malformed_settings_are_rejected(self, settings):
        with pytest.raises(ValueError):
            WorkerConfig.from_dict(settings)


class TestPolicyBudgets:
    def test_database_budgets_are_part_of_the_task_policy(self):
        @dewey.task("reports.build", statement_timeout_s=30, lock_timeout_s=2.5)
        def build():
            pass

        policy = dewey.resolve_policy("reports.build")
        assert (policy.statement_timeout_s, policy.lock_timeout_s) == (30, 2.5)
        assert dewey.resolve_policy("other").statement_timeout_s is None

    @pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), "30", True, 2147484])
    @pytest.mark.parametrize("budget", ["statement_timeout_s", "lock_timeout_s"])
    def test_budgets_must_be_positive_seconds_the_database_can_hold(self, budget, value):
        with pytest.raises(ValueError, match=budget):
            dewey.task("bad", **{budget: value})
        with pytest.raises(ValueError, match=budget):
            dewey.configure_policies({"bad": {budget: value}})
        assert dewey.resolve_policy("bad").statement_timeout_s is None

    def test_the_project_layer_outranks_the_decorator_one_budget_at_a_time(self):
        try:
            dewey.task("reports.build", statement_timeout_s=1, lock_timeout_s=0.1)(lambda: None)
            dewey.configure_policies({"reports.build": {"statement_timeout_s": 2}})
            policy = dewey.resolve_policy("reports.build")
            assert (policy.statement_timeout_s, policy.lock_timeout_s) == (2, 0.1)
        finally:
            dewey.clear_project_policies()


def test_the_handler_sees_its_deadline_budgets_and_trace():
    seen = {}

    @dewey.task("reports.build", statement_timeout_s=30)
    def build():
        task = dewey.current_task()
        seen.update(task=task, remaining=task.remaining(), trace=get_trace_context())

    deadline = datetime.now(UTC) + timedelta(minutes=5)

    class Storage:
        scopes = []

        def call(self, operation, *args):
            if operation == "start":
                return {
                    "task_type": "reports.build",
                    "args": [],
                    "kwargs": {},
                    "execution_token": "token",
                    "attempts": 1,
                    "execution_deadline_at": deadline,
                    "trace": {"request_id": "req-1"},
                }
            return None

    runtime = LedgerRuntime(Storage(), close_resources=lambda: None)
    try:
        runtime.execute(Assignment("task-1", "dispatch-1"))
    finally:
        runtime.close()
    task = seen["task"]
    assert (task.deadline, task.statement_timeout_s, task.lock_timeout_s) == (deadline, 30, None)
    assert timedelta(minutes=4) < seen["remaining"] <= timedelta(minutes=5)
    assert seen["trace"] == {"request_id": "req-1"}
    assert get_trace_context() == {}  # restored once the handler returns
    # Inline execution has no worker and so no deadline.
    assert dewey.TaskContext("t", "x", 1).remaining() is None
    past = dewey.TaskContext("t", "x", 1, deadline=datetime.now(UTC) - timedelta(seconds=1))
    assert past.remaining() == timedelta(0)


def test_time_left_is_counted_from_the_database_clock_not_the_hosts():
    # A host whose clock is an hour behind must not believe it has an hour more.
    stale_wall_clock = datetime.now(UTC) + timedelta(hours=1)
    task = dewey.TaskContext("t", "x", 1, deadline=stale_wall_clock, _stops_at=time.monotonic() + 5)
    assert timedelta(seconds=4) < task.remaining() <= timedelta(seconds=5)


class TestAlertHook:
    def test_hook_is_set_loaded_and_removed(self):
        heard = []
        assert dewey.on_alert(heard.append) is not None
        alerts.emit("task_dead", task_id="t")
        assert heard == [{"event": "task_dead", "task_id": "t"}]
        alerts.on_alert(None)
        alerts.emit("task_dead", task_id="t")
        assert len(heard) == 1
        alerts.load_alert_hook(None)  # nothing configured: left alone
        alerts.load_alert_hook("json:dumps")
        assert alerts._hook is __import__("json").dumps

    @pytest.mark.parametrize("configured", [True, False])
    def test_a_task_process_uses_the_configured_hook_or_keeps_the_one_set_in_code(
        self, monkeypatch, configured
    ):
        import json
        import sys
        from types import ModuleType, SimpleNamespace

        from dewey.worker import WorkerApplication
        from dewey.worker._process import OPTIONS, create_runtime

        by_path, in_code = [], []

        def create(context):
            if not configured:
                dewey.on_alert(in_code.append)  # what an application module does at import
            return WorkerApplication(SimpleNamespace(call=lambda *args: None), lambda: None)

        module = ModuleType("alerting_app")
        module.create, module.notify = create, by_path.append
        monkeypatch.setitem(sys.modules, "alerting_app", module)
        config = WorkerConfig(
            "alerting_app:create", alert_hook="alerting_app:notify" if configured else None
        )
        monkeypatch.setenv(OPTIONS, json.dumps({"config": config.to_dict(), "owner": "o" * 36}))
        create_runtime().close()
        alerts.emit("task_dead", task_id="t")
        heard, silent = (by_path, in_code) if configured else (in_code, by_path)
        assert heard == [{"event": "task_dead", "task_id": "t"}] and silent == []

    @pytest.mark.parametrize("path", ["nocolon", ":function", "module:"])
    def test_a_malformed_hook_path_is_rejected(self, path):
        with pytest.raises(ValueError):
            alerts.load_alert_hook(path)

    def test_a_non_callable_hook_is_rejected(self):
        with pytest.raises(TypeError):
            dewey.on_alert("not callable")  # type: ignore[arg-type]


class TestSQLAlchemyTaskTimeouts:
    """SQLAlchemy handlers own their connections, so they apply the budgets themselves."""

    def test_budgets_are_transaction_local(self, engine):
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from dewey.context import _bind_task
        from dewey.sqlalchemy import apply_task_timeouts

        def setting(session, name):
            return session.execute(text(f"SHOW {name}")).scalar_one()

        task = dewey.TaskContext("t", "reports.build", 1, statement_timeout_s=2.5, lock_timeout_s=1)
        with Session(engine) as session:
            before = setting(session, "statement_timeout")
            session.rollback()
            with session.begin(), _bind_task(task):
                apply_task_timeouts(session)
                assert setting(session, "statement_timeout") == "2500ms"
                assert setting(session, "lock_timeout") == "1s"
            assert setting(session, "statement_timeout") == before  # ended with the transaction
            session.rollback()
            with session.begin(), _bind_task(dewey.TaskContext("t", "x", 1)):
                apply_task_timeouts(session)  # no budgets: nothing changes
                assert setting(session, "statement_timeout") == before

    def test_outside_a_handler_is_an_error(self, session):
        from dewey.sqlalchemy import apply_task_timeouts

        with pytest.raises(RuntimeError):
            apply_task_timeouts(session)

    @pytest.mark.asyncio
    async def test_async_sessions(self, async_session):
        from sqlalchemy import text

        from dewey.context import _bind_task
        from dewey.sqlalchemy import apply_task_timeouts_async

        with _bind_task(dewey.TaskContext("t", "x", 1, lock_timeout_s=3)):
            await apply_task_timeouts_async(async_session)
        assert (await async_session.execute(text("SHOW lock_timeout"))).scalar_one() == "3s"
        await async_session.rollback()
