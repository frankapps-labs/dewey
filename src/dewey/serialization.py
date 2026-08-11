"""Encode task arguments to JSON that Postgres — and a human — can read.

Task arguments are persisted, so they have to survive a trip through JSON. Dewey
converts the four rich types that show up constantly in application code, and
refuses everything it cannot represent honestly:

===============  ==========================  ================================
Python           Stored as                   Handler receives
===============  ==========================  ================================
``datetime``     ISO 8601 string             ``str`` — parse with
                                             ``datetime.fromisoformat``
``date``         ISO 8601 string             ``str``
``time``         ISO 8601 string             ``str``
``UUID``         canonical string            ``str``
``Decimal``      decimal string              ``str`` — rebuild with
                                             ``Decimal(value)``
``Enum``         the member's value          the member's value
===============  ==========================  ================================

The conversion is deliberately **lossy**: a handler annotated
``def send(due_at: datetime)`` receives a string, not a ``datetime``. In exchange,
a row stays readable in ``psql``, the encoding is language-neutral, and Dewey
never has to teach every future reader a private tagging format. Handlers that
want the rich type parse one line at the top — the same trade Celery's JSON
serializer makes.

``bytes`` and ORM instances are rejected outright, with a message that says what
to do instead.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from dewey.errors import SerializationError

#: Depth guard. Deeper than this and the argument is a document, not a call
#: argument — pass an ID and let the handler load it.
MAX_DEPTH = 10


def _fail(path: str, value: Any, hint: str) -> SerializationError:
    return SerializationError(
        f"Cannot serialize {path}: {type(value).__name__} is not JSON-safe. {hint}"
    )


def encode_value(value: Any, *, path: str = "value", _depth: int = 0) -> Any:
    """Convert one value to something JSON can hold. Raises SerializationError."""
    if _depth > MAX_DEPTH:
        raise SerializationError(
            f"Cannot serialize {path}: nested deeper than {MAX_DEPTH} levels. "
            f"Task arguments are call arguments, not documents — pass an ID and let "
            f"the handler load the rest."
        )

    if value is None or isinstance(value, str | bool | int):
        return value

    if isinstance(value, float):
        # NaN and infinities are not valid JSON and Postgres will reject them.
        if value != value or value in (float("inf"), float("-inf")):
            raise SerializationError(
                f"Cannot serialize {path}: {value!r} has no JSON representation."
            )
        return value

    if isinstance(value, bytes | bytearray | memoryview):
        raise _fail(
            path,
            value,
            "Store the blob somewhere addressable (object storage, a table) and pass "
            "the key or URL instead.",
        )

    if isinstance(value, Enum):
        return encode_value(value.value, path=path, _depth=_depth + 1)

    if isinstance(value, datetime | date | time):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise SerializationError(
                f"Cannot serialize {path}: {value!r} has no JSON representation."
            )
        return str(value)

    if isinstance(value, Mapping):
        encoded: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise SerializationError(
                    f"Cannot serialize {path}: mapping keys must be strings, got "
                    f"{type(key).__name__} ({key!r}). JSON would silently stringify it "
                    f"and the handler would receive a different key type than you passed."
                )
            encoded[key] = encode_value(item, path=f"{path}[{key!r}]", _depth=_depth + 1)
        return encoded

    if isinstance(value, set | frozenset):
        raise _fail(
            path,
            value,
            "Sets have no defined order in JSON. Pass a sorted list so the handler "
            "sees the same sequence you did.",
        )

    if isinstance(value, Sequence):
        return [
            encode_value(item, path=f"{path}[{index}]", _depth=_depth + 1)
            for index, item in enumerate(value)
        ]

    raise _fail(
        path,
        value,
        "Pass a scalar the handler can look up — an ID, not the object. Handlers load "
        "their own domain rows.",
    )


def encode_args(args: Sequence[Any] | None, *, path: str = "args") -> list[Any]:
    """Encode positional arguments. Returns a JSON-safe list."""
    if args is None:
        return []
    if isinstance(args, str | bytes) or not isinstance(args, Sequence):
        raise SerializationError(
            f"{path} must be a sequence of positional arguments, got "
            f"{type(args).__name__}. Pass args=[value] rather than args=value."
        )
    return [encode_value(item, path=f"{path}[{index}]") for index, item in enumerate(args)]


def encode_kwargs(kwargs: Mapping[str, Any] | None, *, path: str = "kwargs") -> dict[str, Any]:
    """Encode keyword arguments. Returns a JSON-safe dict."""
    if kwargs is None:
        return {}
    if not isinstance(kwargs, Mapping):
        raise SerializationError(
            f"{path} must be a mapping of keyword arguments, got {type(kwargs).__name__}."
        )
    encoded: dict[str, Any] = {}
    for key, value in kwargs.items():
        if not isinstance(key, str):
            raise SerializationError(
                f"{path} keys must be strings — they become keyword argument names. "
                f"Got {type(key).__name__} ({key!r})."
            )
        encoded[key] = encode_value(value, path=f"{path}[{key!r}]")
    return encoded


def dumps(value: Any) -> str:
    """JSON-encode an already-encoded structure, rejecting NaN and infinities."""
    return json.dumps(value, allow_nan=False, separators=(",", ":"))


__all__ = ["MAX_DEPTH", "dumps", "encode_args", "encode_kwargs", "encode_value"]
