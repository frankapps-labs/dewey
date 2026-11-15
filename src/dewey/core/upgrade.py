"""One-time data steps for upgrading an existing ledger between releases."""

# 0.6 removed broker dispatch. A row the old path left DISPATCHING or PROCESSING
# has no native ownership, so nothing would ever recover it: hand it back as
# PENDING. A PROCESSING row gets its interrupted attempt back.
RESET_LEGACY_CLAIMS_SQL = """
UPDATE task_entries
SET status = 'pending',
    attempts = CASE WHEN status = 'processing' THEN GREATEST(attempts - 1, 0) ELSE attempts END,
    execution_token = NULL,
    dispatching_at = NULL,
    started_at = NULL,
    updated_at = clock_timestamp()
WHERE status IN ('dispatching', 'processing')
  AND dispatch_token IS NULL
  AND worker_id IS NULL
  AND lease_expires_at IS NULL
  AND execution_deadline_at IS NULL
"""

__all__ = ["RESET_LEGACY_CLAIMS_SQL"]
