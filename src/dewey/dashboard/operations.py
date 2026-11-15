"""Typed, fail-closed host policies. Never classify arbitrary error text."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from urllib.parse import urlsplit


class FailureCategory(StrEnum):
    UNKNOWN = "unknown"
    TRANSIENT = "transient"
    BUSINESS = "business"
    VALIDATION = "validation"


def safe_link(value: str, origins: tuple[str, ...] = (), *, business: bool = False) -> str:
    """Accept local absolute paths or explicit HTTPS origins; no URL credentials.

    Query/fragment URLs are excluded rather than attempting secret redaction.
    Hosts own both the allowlist and reviewed non-sensitive path contents.
    """
    if not isinstance(value, str) or len(value) > 1000:
        raise ValueError("Invalid diagnostic link")
    if not value:
        return ""
    if any(ord(c) <= 32 or ord(c) == 127 for c in value) or "\\" in value or "%" in value:
        raise ValueError("Invalid diagnostic link")
    parts = urlsplit(value)
    if parts.query or parts.fragment or parts.username or parts.password:
        raise ValueError("Invalid diagnostic link")
    if business and value.startswith("/") and not value.startswith("//") and not parts.netloc:
        return value
    if not business and parts.scheme == "https" and f"https://{parts.netloc}" in origins:
        return value
    raise ValueError("Invalid diagnostic link")


@dataclass(frozen=True)
class Diagnostics:
    """Host-reviewed data for exactly one outcome event/attempt, not raw errors.

    A transient classification alone does NOT authorize replay. Provider refs
    are optional: killed/offline workers commonly have no remote event.
    """

    category: FailureCategory = FailureCategory.UNKNOWN
    summary: str = ""
    business_label: str = ""
    business_url: str = ""
    provider: str = ""
    reference: str = ""
    reference_url: str = ""

    def validated(self, origins: tuple[str, ...] = ()) -> dict:
        if not isinstance(self.category, FailureCategory):
            raise ValueError("Use a typed FailureCategory")
        for value, limit in (
            (self.summary, 500),
            (self.business_label, 100),
            (self.reference, 200),
        ):
            if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 for c in value):
                raise ValueError("Invalid reviewed diagnostic text")
        if self.provider not in ("", "sentry", "logfire"):
            raise ValueError("Unsupported diagnostic provider")
        if (self.reference or self.reference_url) and not self.provider:
            raise ValueError("Reference requires a provider")
        safe_link(self.business_url, business=True)
        safe_link(self.reference_url, origins)
        return asdict(self)


@dataclass(frozen=True)
class RetryDecision:
    """An explicit host verdict on current eligibility/recovery/effect safety.

    Approval means replay on the SAME task/operation keys is safe now, including
    any external receipt retention and domain expiry. Must be local, bounded,
    side-effect-free. False is the default; exceptions also deny.
    """

    allowed: bool = False
    explanation: str = "Retry is not approved by the host policy."

    def validated(self) -> RetryDecision:
        if type(self.allowed) is not bool or not isinstance(self.explanation, str):
            raise ValueError("Invalid retry decision")
        if not 1 <= len(self.explanation) <= 500 or any(ord(c) < 32 for c in self.explanation):
            raise ValueError("Invalid retry explanation")
        return self
