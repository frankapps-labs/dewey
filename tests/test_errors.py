"""Tests for typed errors — currently the 0.5 idempotency conflict contract."""

import pytest

import dewey
from dewey.errors import DeweyError, IdempotencyConflictError


class TestIdempotencyConflictError:
    def test_is_a_dewey_error(self):
        err = IdempotencyConflictError(("args",))
        assert isinstance(err, DeweyError)

    def test_differing_fields_is_an_immutable_tuple(self):
        err = IdempotencyConflictError(["queue", "priority"])
        assert err.differing_fields == ("queue", "priority")
        assert isinstance(err.differing_fields, tuple)

    def test_preserves_field_order(self):
        err = IdempotencyConflictError(("task_type", "args", "kwargs"))
        assert err.differing_fields == ("task_type", "args", "kwargs")

    def test_message_names_the_differing_fields(self):
        err = IdempotencyConflictError(("args", "max_attempts"))
        assert "args" in str(err)
        assert "max_attempts" in str(err)

    def test_message_never_carries_argument_values(self):
        """The constructor accepts only field names — conflicting values (which
        may hold payload data) cannot reach the message or the exception args."""
        err = IdempotencyConflictError(("kwargs",))
        assert err.args == (str(err),)
        assert str(err) == (
            "idempotency key matched an existing task, but these immutable "
            "creation fields differ: kwargs"
        )

    def test_requires_at_least_one_field(self):
        """A conflict that differs in nothing is a bug in the caller."""
        with pytest.raises(ValueError):
            IdempotencyConflictError(())

    def test_is_exported_from_the_package_root(self):
        assert dewey.IdempotencyConflictError is IdempotencyConflictError
        assert "IdempotencyConflictError" in dewey.__all__
