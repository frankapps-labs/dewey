"""Typed errors — how a handler tells Dewey what should happen next.

Handlers never sleep, never retry themselves, and never touch the ledger. They
raise, and the policy decides:

- :class:`TransientError` — this attempt failed for a reason that may pass.
  Retried per the task's backoff policy until ``max_attempts`` is spent.
- :class:`NonRetryableError` — this will never succeed. Dead-lettered
  immediately, no matter how many attempts remain.
- :class:`RetryAfter` — the handler knows more than the policy about *this*
  attempt (a provider's ``Retry-After`` header, for example). Reschedules no
  earlier than the policy's own backoff would allow.
"""

from __future__ import annotations


class DeweyError(Exception):
    """Base class for every error Dewey raises or interprets."""


class TransientError(DeweyError):
    """The attempt failed, but a later attempt might succeed."""


class NonRetryableError(DeweyError):
    """The task can never succeed. Dead-letter it now, ignoring remaining attempts."""


class RetryAfter(DeweyError):
    """Retry no earlier than ``seconds`` from now.

    The scheduled time is ``max(seconds, policy_backoff)`` — a handler can ask
    for *later* than the policy would, never *earlier*. That keeps a
    misbehaving external API from driving retries past your own rate budget.

    Args:
        seconds: How long the handler wants to wait. Must not be negative.
        message: Optional detail recorded on the task row.
    """

    def __init__(self, seconds: float, message: str = "") -> None:
        if seconds < 0:
            raise ValueError(f"RetryAfter seconds must not be negative, got {seconds!r}")
        self.seconds = float(seconds)
        self.message = message
        super().__init__(message or f"retry after {self.seconds:g}s")


class SerializationError(DeweyError, TypeError):
    """A task argument cannot be persisted as JSON."""


class DuplicateTaskTypeError(DeweyError):
    """Two different handlers were registered for the same task type."""


class UnknownTaskTypeError(DeweyError, LookupError):
    """No handler is registered for a task type the worker was asked to run."""


__all__ = [
    "DeweyError",
    "DuplicateTaskTypeError",
    "NonRetryableError",
    "RetryAfter",
    "SerializationError",
    "TransientError",
    "UnknownTaskTypeError",
]
