"""Version-1 dashboard JSON protocol; all timestamps are ISO-8601 UTC strings."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from dewey.core.states import TaskStatus


@dataclass(frozen=True)
class TaskFilter:
    """Bounded, validated inputs shared by framework adapters.

    The window scopes task creation time, including summaries and owner observations.
    Pagination is by (created_at, id), not increasingly expensive SQL offsets.
    """

    hours: int = 24
    limit: int = 25
    queue: str = ""
    status: str = ""
    search: str = ""
    cursor: tuple[datetime, str] | None = None

    @classmethod
    def parse(cls, params: Any) -> TaskFilter:
        try:
            hours = int(params.get("hours", 24))
            limit = int(params.get("limit", 25))
            queue = str(params.get("queue", "")).strip()
            status = str(params.get("status", "")).strip()
            search = str(params.get("search", "")).strip()
            if hours not in (1, 24, 168) or not 1 <= limit <= 100:
                raise ValueError
            if len(queue) > 50 or len(search) > 100:
                raise ValueError
            if status and status not in {s.value for s in TaskStatus}:
                raise ValueError
            cursor = None
            raw = params.get("cursor", "")
            if raw:
                if not isinstance(raw, str) or len(raw) > 256:
                    raise ValueError
                stamp, row_id = json.loads(base64.b64decode(raw, altchars=b"-_", validate=True))
                date = datetime.fromisoformat(stamp)
                if date.tzinfo is None or not isinstance(row_id, str) or not 1 <= len(row_id) <= 36:
                    raise ValueError
                cursor = (date.astimezone(UTC), row_id)
            return cls(hours, limit, queue, status, search, cursor)
        except (ValueError, TypeError, KeyError, binascii.Error, OverflowError) as exc:
            raise ValueError("Invalid dashboard filters") from exc


def next_cursor(created_at: datetime, row_id: str) -> str:
    return base64.urlsafe_b64encode(json.dumps([created_at.isoformat(), row_id]).encode()).decode()


class DashboardProvider(Protocol):
    """Host implements these read-only methods, returning contract-v1 JSON objects.

    overview: version, observed_at, hours, summary, queues, owners, limitations, throughput.
    Typed diagnostics/retry verdicts extend detail; optional commands are host-owned.
    tasks: version, items, next_cursor. detail: version, task (or None).
    See docs/dashboard.md for fields, access policy and observation semantics.
    """

    def overview(self, filters: TaskFilter) -> dict[str, Any]: ...

    def tasks(self, filters: TaskFilter) -> dict[str, Any]: ...

    def detail(self, task_id: str) -> dict[str, Any] | None: ...
