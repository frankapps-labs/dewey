"""One-time data steps for upgrading an existing ledger (Django runs these as migrations)."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dewey.core.upgrade import RESET_LEGACY_CLAIMS_SQL


def reset_legacy_claims(session: Session) -> int:
    """0.5 → 0.6: release rows the removed broker path left claimed.

    Stop the old workers first. Returns the number of rows handed back; the
    caller commits.
    """
    return session.execute(text(RESET_LEGACY_CLAIMS_SQL)).rowcount  # type: ignore[attr-defined]


async def reset_legacy_claims_async(session: AsyncSession) -> int:
    return (await session.execute(text(RESET_LEGACY_CLAIMS_SQL))).rowcount  # type: ignore[attr-defined]


__all__ = ["reset_legacy_claims", "reset_legacy_claims_async"]
