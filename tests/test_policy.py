"""Tests for the task policy chain: declaration, precedence, and classification."""

from __future__ import annotations

from datetime import timedelta

import pytest

import dewey
from dewey.errors import DuplicateTaskTypeError, NonRetryableError, TransientError
from dewey.policy import (
    TASK_DEFAULTS,
    Constant,
    Custom,
    Exponential,
    PolicyRegistry,
    TaskPolicy,
    clear_project_policies,
    configure_policies,
    registry,
    resolve_policy,
    task,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    registry.clear()
    clear_project_policies()
    yield
    registry.clear()
    clear_project_policies()


class TestBackoff:
    def test_constant_is_flat(self):
        policy = Constant(3)
        assert policy.delay_for(1) == timedelta(seconds=3)
        assert policy.delay_for(7) == timedelta(seconds=3)

    def test_exponential_doubles_from_base(self):
        policy = Exponential(base_s=10, factor=2, cap_s=1000, jitter=0)
        assert policy.delay_for(1) == timedelta(seconds=10)
        assert policy.delay_for(2) == timedelta(seconds=20)
        assert policy.delay_for(3) == timedelta(seconds=40)

    def test_exponential_caps(self):
        policy = Exponential(base_s=10, factor=10, cap_s=100, jitter=0)
        assert policy.delay_for(5) == timedelta(seconds=100)

    def test_exponential_jitter_stays_within_cap_and_non_negative(self):
        policy = Exponential(base_s=10, factor=2, cap_s=30, jitter=1.0)
        for attempt in range(1, 10):
            delay = policy.delay_for(attempt).total_seconds()
            assert 0 <= delay <= 30

    def test_custom_accepts_seconds_or_timedelta(self):
        assert Custom(lambda attempt: attempt * 2).delay_for(3) == timedelta(seconds=6)
        assert Custom(lambda attempt: timedelta(minutes=attempt)).delay_for(2) == timedelta(
            minutes=2
        )


class TestDecoratorRegistration:
    def test_registers_handler_and_policy(self):
        @task("orders.confirm", max_attempts=2, queue="critical", priority=7)
        def confirm(order_id: int) -> str:
            return f"ok-{order_id}"

        policy = resolve_policy("orders.confirm")
        assert policy.handler is confirm
        assert policy.max_attempts == 2
        assert policy.queue == "critical"
        assert policy.priority == 7

    def test_returns_the_function_unchanged(self):
        @task("orders.confirm")
        def confirm(order_id: int) -> str:
            return f"ok-{order_id}"

        # Still an ordinary function: directly callable, directly testable.
        assert confirm(7) == "ok-7"

    def test_unregistered_type_resolves_to_defaults_without_handler(self):
        policy = resolve_policy("never.declared")
        assert policy.handler is None
        assert policy.max_attempts == TASK_DEFAULTS.max_attempts
        assert policy.task_type == "never.declared"

    def test_duplicate_task_type_with_a_different_handler_is_refused(self):
        @task("orders.confirm")
        def first(order_id: int) -> None: ...

        with pytest.raises(DuplicateTaskTypeError) as excinfo:

            @task("orders.confirm")
            def second(order_id: int) -> None: ...

        message = str(excinfo.value)
        assert "orders.confirm" in message
        assert "first" in message and "second" in message

    def test_re_registering_the_same_declaration_is_idempotent(self):
        """A module imported twice must not explode at import time."""

        def handler(order_id: int) -> None: ...

        policy = TaskPolicy(task_type="orders.confirm", handler=handler)
        local = PolicyRegistry()
        local.register(policy)
        local.register(TaskPolicy(task_type="orders.confirm", handler=handler))
        assert local.task_types() == ("orders.confirm",)

    def test_replace_existing_allows_a_deliberate_override(self):
        local = PolicyRegistry()
        local.register(TaskPolicy(task_type="t", handler=lambda: None))
        replacement = TaskPolicy(task_type="t", handler=lambda: None, max_attempts=9)
        local.register(replacement, replace_existing=True)
        assert local.get("t") is replacement

    def test_unknown_policy_field_fails_at_declaration_time(self):
        with pytest.raises(TypeError, match="Unknown policy field"):

            @task("orders.confirm", max_attemps=3)  # typo
            def confirm() -> None: ...

    @pytest.mark.parametrize("bad", [0, -1, "3", True])
    def test_invalid_max_attempts_is_refused(self, bad):
        with pytest.raises((TypeError, ValueError)):

            @task("orders.confirm", max_attempts=bad)
            def confirm() -> None: ...

    def test_bare_exception_class_is_accepted_for_retry_on(self):
        @task("orders.confirm", retry_on=ConnectionError)
        def confirm() -> None: ...

        assert resolve_policy("orders.confirm").retry_on == (ConnectionError,)

    def test_backoff_must_implement_delay_for(self):
        with pytest.raises(TypeError, match="delay_for"):

            @task("orders.confirm", backoff=3)
            def confirm() -> None: ...


class TestPrecedence:
    def test_project_config_outranks_the_decorator(self):
        @task("orders.confirm", max_attempts=2, queue="fast")
        def confirm() -> None: ...

        configure_policies({"orders.confirm": {"max_attempts": 10}})

        policy = resolve_policy("orders.confirm")
        assert policy.max_attempts == 10  # project layer wins
        assert policy.queue == "fast"  # untouched fields keep the decorator hint
        assert policy.handler is confirm  # the handler is never overridable

    def test_decorator_outranks_defaults(self):
        @task("orders.confirm", max_attempts=2)
        def confirm() -> None: ...

        assert TASK_DEFAULTS.max_attempts != 2
        assert resolve_policy("orders.confirm").max_attempts == 2

    def test_project_config_applies_to_undeclared_types(self):
        """Policy may be configured before the handler module is imported."""
        configure_policies({"orders.confirm": {"queue": "critical"}})
        assert resolve_policy("orders.confirm").queue == "critical"

    def test_project_config_rejects_unknown_fields(self):
        with pytest.raises(TypeError, match="Unknown policy field"):
            configure_policies({"orders.confirm": {"nope": 1}})

    def test_project_config_is_validated_before_anything_is_applied(self):
        configure_policies({"a": {"queue": "one"}})
        with pytest.raises(TypeError):
            configure_policies({"a": {"queue": "two"}, "b": {"bogus": 1}})
        assert resolve_policy("a").queue == "one"


class TestFailureClassification:
    def test_any_exception_retries_by_default(self):
        policy = TaskPolicy(task_type="t")
        assert policy.is_retryable(ValueError("boom")) is True
        assert policy.is_retryable(TransientError("blip")) is True

    def test_non_retryable_error_always_fails_fast(self):
        policy = TaskPolicy(task_type="t")
        assert policy.is_retryable(NonRetryableError("bad request")) is False

    def test_retry_on_narrows_to_listed_exceptions(self):
        policy = TaskPolicy(task_type="t", retry_on=(ConnectionError,))
        assert policy.is_retryable(ConnectionError()) is True
        assert policy.is_retryable(ValueError()) is False

    def test_fail_fast_on_beats_retry_on(self):
        class Gone(ConnectionError):
            pass

        policy = TaskPolicy(task_type="t", retry_on=(ConnectionError,), fail_fast_on=(Gone,))
        assert policy.is_retryable(ConnectionError()) is True
        assert policy.is_retryable(Gone()) is False


class TestPublicSurface:
    def test_decorator_and_policy_types_are_exported_from_the_package_root(self):
        assert dewey.task is task
        assert dewey.Constant is Constant
        assert dewey.TaskPolicy is TaskPolicy
        assert issubclass(dewey.RetryAfter, dewey.DeweyError)
