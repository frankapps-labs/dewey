# Query API

## Query API

All query functions return `TaskEntry` dataclasses — pure Python, no ORM dependency in consumers.

SQLAlchemy functions take `session` as the first argument. Django functions take no session.

| Function | Description |
|----------|-------------|
| `get_stats(...)` | Counts by status |
| `get_pending(...)` | Tasks waiting to process |
| `get_processing(...)` | Currently processing |
| `get_stuck(...)` | Processing too long |
| `get_failed(...)` | Failed, eligible for retry |
| `get_dead(...)` | Dead-lettered |
| `get_task(..., id)` | Single task detail |
| `get_recent(...)` | Recent with filters |
| `retry_task(..., id)` | Reset failed/dead → pending |
| `bulk_retry(...)` | Retry all failed |
| `kill_task(..., id)` | Force to dead |
| `purge_completed(...)` | Delete old completed |
