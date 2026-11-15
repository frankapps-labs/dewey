"""Portable retention settings and explicit host safety verdicts (no ORM)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionConfig:
    """One completed-task policy for a database, not a current dashboard filter."""

    days: int = 30
    enabled: bool = False
    revision: int = 0

    def __post_init__(self):
        if type(self.days) is not int or not 1 <= self.days <= 3650:
            raise ValueError("Retention days must be an integer between 1 and 3650.")
        if type(self.enabled) is not bool or type(self.revision) is not int or self.revision < 0:
            raise ValueError("Invalid retention configuration.")


@dataclass(frozen=True)
class RetentionDecision:
    """Host-owned approval, never inferred from age or an operator checkbox.

    Approval covers ALL task types in this database: creation-key/effect replay
    protection survives deletion, and discarded transition history is acceptable
    or preserved elsewhere. It also covers application-owned deletion side effects.
    The callable must be local, bounded and side-effect-free.
    """

    deduplication_safe: bool = False
    history_disposal_safe: bool = False
    minimum_days: int = 30
    explanation: str = "The host has not approved history and deduplication disposal."

    def validated(self) -> RetentionDecision:
        if (
            type(self.deduplication_safe) is not bool
            or type(self.history_disposal_safe) is not bool
        ):
            raise ValueError("Invalid retention safety verdict.")
        if type(self.minimum_days) is not int or not 1 <= self.minimum_days <= 3650:
            raise ValueError("Invalid minimum retention.")
        if (
            not isinstance(self.explanation, str)
            or not 1 <= len(self.explanation) <= 500
            or any(ord(c) < 32 for c in self.explanation)
        ):
            raise ValueError("Invalid retention explanation.")
        return self

    def allows(self, config: RetentionConfig) -> bool:
        return (
            self.deduplication_safe
            and self.history_disposal_safe
            and config.days >= self.minimum_days
        )
