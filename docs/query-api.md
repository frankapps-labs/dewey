# Query API

Operational access to the ledger: what is waiting, what is stuck, what died, and what to
do about it. Every function returns `TaskEntry` dataclasses — pure Python, so nothing in
your code depends on Dewey's ORM choice.

SQLAlchemy functions take `session` first, with `_async` variants for the async API.
Django functions take no session.

```python
from dewey.sqlalchemy import get_stats, get_dead, retry_task   # session-first
from dewey.django import get_stats, get_dead, retry_task       # no session
```

## Inspecting

| Function | Returns |
|---|---|
| `get_stats(...)` | Counts per status, including zeros |
| `get_pending(..., limit=50)` | Waiting to be claimed, oldest first |
| `get_dispatching(..., limit=50)` | Claimed and handed to the transport, no worker started yet |
| `get_processing(..., limit=50)` | Currently running |
| `get_stuck(..., older_than_minutes=10)` | `PROCESSING` for too long — sweep candidates |
| `get_failed(..., limit=50)` | Failed, awaiting their retry time |
| `get_dead(..., limit=50)` | Dead-lettered, needing a human |
| `get_task(..., task_id)` | One row in full |
| `get_recent(..., limit=50)` | Recent activity, filtered |

```python
get_stats()
# {'pending': 3, 'dispatching': 1, 'processing': 2, 'completed': 4891, 'failed': 0, 'dead': 1}
```

Reading these together is usually how you diagnose:

- **`pending` climbing** — no dispatcher running, or it cannot keep up.
- **`dispatching` climbing** — the transport took the IDs but workers are not picking them
  up: no consumer running, or the pool is saturated.
- **`processing` stuck on a fixed set** — handlers are hanging. `get_stuck()` names them.
- **`dead` growing** — a real failure to look at; `error` and `attempts` are on the row.

## Intervening

| Function | Effect |
|---|---|
| `retry_task(..., task_id)` | `FAILED`/`DEAD` → `PENDING`, dispatched on the next claim |
| `bulk_retry(...)` | The same, for every retryable row |
| `kill_task(..., task_id)` | Force → `DEAD`, respected even mid-processing |
| `purge_completed(..., older_than_days=30)` | Delete old `COMPLETED` rows |

`kill_task` on a running task is honoured: the worker notices its status changed underneath
and declines to overwrite the kill.

Retention is yours to schedule — `purge_completed` is a function, not a background job.
Call it from cron or a management command to suit your retention policy. `COMPLETED` rows
otherwise accumulate forever, which is good for audit and bad for table size.

## Straight SQL

The point of keeping the backlog in Postgres is that you do not need an API for it:

```sql
-- Backlog by type and queue
SELECT task_type, queue, count(*) FROM task_entries
WHERE status = 'pending' GROUP BY 1, 2 ORDER BY 3 DESC;

-- What died today, and why
SELECT task_type, error, attempts, updated_at FROM task_entries
WHERE status = 'dead' AND updated_at > now() - interval '1 day';

-- Work waiting on a schedule
SELECT id, task_type, scheduled_for FROM task_entries
WHERE status = 'pending' AND scheduled_for > now() ORDER BY scheduled_for;

-- End-to-end latency of recent completions
SELECT task_type,
       percentile_disc(0.95) WITHIN GROUP (ORDER BY completed_at - created_at) AS p95
FROM task_entries
WHERE status = 'completed' AND completed_at > now() - interval '1 hour'
GROUP BY 1;
```

Per-tenant filtering works the same way when the identifier is in `args`, `kwargs` or
`metadata`. Those are `JSON` columns; on Postgres you can convert them to `JSONB` and
index them:

```sql
ALTER TABLE task_entries ALTER COLUMN kwargs TYPE jsonb USING kwargs::jsonb;
CREATE INDEX ix_task_entries_kwargs ON task_entries USING gin (kwargs);
```
