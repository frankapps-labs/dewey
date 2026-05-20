"""Shared test helpers — fake channels for notification tests."""

from __future__ import annotations

from typing import Any

from dewey.core.notifications import ChannelResult


class FakeChannel:
    """A test channel that records calls and returns configurable results."""

    def __init__(self, name: str = "email", succeed: bool = True, error: str = "fail"):
        self._name = name
        self._succeed = succeed
        self._error = error
        self.calls: list[dict] = []

    @property
    def name(self) -> str:
        return self._name

    def send(
        self, recipient: str, subject: str | None, body: str, payload: dict[str, Any]
    ) -> ChannelResult:
        self.calls.append(
            {"recipient": recipient, "subject": subject, "body": body, "payload": payload}
        )
        if self._succeed:
            return ChannelResult(success=True, response_data={"message_id": "msg-1"})
        return ChannelResult(success=False, error=self._error)


class RaisingChannel:
    """A test channel that always raises an exception."""

    @property
    def name(self) -> str:
        return "broken"

    def send(
        self, recipient: str, subject: str | None, body: str, payload: dict[str, Any]
    ) -> ChannelResult:
        raise ConnectionError("channel exploded")
