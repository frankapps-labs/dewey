"""Argument serialization: what Dewey converts, and what it refuses."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum, IntEnum
from uuid import UUID

import pytest

from dewey.errors import SerializationError
from dewey.serialization import MAX_DEPTH, dumps, encode_args, encode_kwargs, encode_value


class Colour(Enum):
    RED = "red"


class Level(IntEnum):
    HIGH = 3


class TestPassThrough:
    @pytest.mark.parametrize("value", [None, True, False, 0, -1, 42, 3.5, "", "text"])
    def test_json_native_values_are_unchanged(self, value):
        assert encode_value(value) == value


class TestRichTypes:
    def test_datetime_becomes_iso_8601(self):
        moment = datetime(2026, 7, 30, 9, 15, tzinfo=UTC)
        assert encode_value(moment) == "2026-07-30T09:15:00+00:00"

    def test_date_and_time_become_iso_8601(self):
        assert encode_value(date(2026, 7, 30)) == "2026-07-30"
        assert encode_value(time(9, 15)) == "09:15:00"

    def test_uuid_becomes_its_canonical_string(self):
        value = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        assert encode_value(value) == "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

    def test_decimal_keeps_its_exact_digits(self):
        """A float would round 19.99; a string does not."""
        assert encode_value(Decimal("19.99")) == "19.99"

    def test_enum_becomes_its_value(self):
        assert encode_value(Colour.RED) == "red"
        assert encode_value(Level.HIGH) == 3

    def test_encoded_output_is_json_serializable(self):
        encoded = encode_kwargs(
            {
                "when": datetime(2026, 7, 30, tzinfo=UTC),
                "who": UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                "amount": Decimal("19.99"),
                "colour": Colour.RED,
            }
        )
        assert dumps(encoded)


class TestContainers:
    def test_nested_containers_are_encoded_throughout(self):
        encoded = encode_value({"items": [{"due": date(2026, 1, 1)}]})
        assert encoded == {"items": [{"due": "2026-01-01"}]}

    def test_tuples_become_lists(self):
        assert encode_value((1, 2)) == [1, 2]

    def test_sets_are_refused_because_order_is_undefined(self):
        with pytest.raises(SerializationError, match="no defined order"):
            encode_value({1, 2, 3})

    def test_non_string_mapping_keys_are_refused(self):
        """JSON would stringify them and the handler would see a different key type."""
        with pytest.raises(SerializationError, match="keys must be strings"):
            encode_value({1: "one"})

    def test_excessive_nesting_is_refused(self):
        deep: object = "leaf"
        for _ in range(MAX_DEPTH + 2):
            deep = [deep]
        with pytest.raises(SerializationError, match="nested deeper"):
            encode_value(deep)


class TestRefusals:
    def test_bytes_are_refused_with_a_usable_hint(self):
        with pytest.raises(SerializationError) as excinfo:
            encode_value(b"blob")
        assert "object storage" in str(excinfo.value)

    def test_arbitrary_objects_are_refused_and_named(self):
        class Order:
            pass

        with pytest.raises(SerializationError) as excinfo:
            encode_value(Order())
        message = str(excinfo.value)
        assert "Order" in message
        assert "ID" in message  # tells the caller to pass an identifier instead

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
    def test_non_finite_floats_are_refused(self, value):
        with pytest.raises(SerializationError, match="no JSON representation"):
            encode_value(value)

    def test_non_finite_decimals_are_refused(self):
        with pytest.raises(SerializationError, match="no JSON representation"):
            encode_value(Decimal("NaN"))

    def test_the_error_names_the_offending_position(self):
        with pytest.raises(SerializationError) as excinfo:
            encode_args([1, {"order": b"blob"}])
        assert "args[1]['order']" in str(excinfo.value)


class TestArgumentShapes:
    def test_none_becomes_empty(self):
        assert encode_args(None) == []
        assert encode_kwargs(None) == {}

    def test_a_bare_value_passed_as_args_is_refused(self):
        """args=42 is a slip that would otherwise be silently iterated or crash later."""
        with pytest.raises(SerializationError, match=r"args=\[value\]"):
            encode_args(42)  # type: ignore[arg-type]

    def test_a_string_passed_as_args_is_refused_rather_than_split(self):
        with pytest.raises(SerializationError):
            encode_args("abc")  # type: ignore[arg-type]

    def test_non_string_kwargs_keys_are_refused(self):
        with pytest.raises(SerializationError, match="keyword argument names"):
            encode_kwargs({1: "one"})  # type: ignore[dict-item]
