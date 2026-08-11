---
name: Bug report
about: Something does not behave as documented
labels: bug
---

## What happened

## What you expected

## Reproduction

Smallest code that shows it, ideally including the `@dewey.task` declaration and the
`create_task` call.

## The row, if work was lost, duplicated or stuck

```sql
SELECT id, task_type, status, attempts, max_attempts, scheduled_for,
       dispatching_at, started_at, completed_at, error
FROM task_entries WHERE id = '...';
```

## Environment

- Dewey version:
- Python version:
- ORM: SQLAlchemy sync / SQLAlchemy async / Django (version)
- Postgres driver: psycopg2 / psycopg3 / asyncpg (version)
- PostgreSQL version:
- Transport: Huey / in-process / other
- Number of dispatcher processes:

## Relevant logs

Lines from the `dewey.*` loggers around the problem.
