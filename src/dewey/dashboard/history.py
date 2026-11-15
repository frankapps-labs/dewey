"""PostgreSQL transition capture shared by both ORM schemas.

Install only during an explicit schema migration, never at worker startup.
No inferred/backfilled history: rows predating installation remain unknown.
"""

# Frozen V1: Django migration0005 imports this. Future schema changes must use
# new statements/migrations, never revise these historical installation semantics.
HISTORY_SQL = (
    "INSERT INTO dewey_history_state (id, started_at) VALUES ('task-events', clock_timestamp()) ON CONFLICT (id) DO NOTHING",
    """
    CREATE OR REPLACE FUNCTION dewey_capture_task_event() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.status IS DISTINCT FROM OLD.status
         AND NEW.status IN ('failed', 'completed', 'dead', 'expired') THEN
        EXECUTE format('INSERT INTO %I.dewey_task_events
          (id, task_id, queue, attempt, kind, occurred_at, diagnostics, audit)
          VALUES ($1, $2, $3, $4, $5, clock_timestamp(), $6, $7)', TG_TABLE_SCHEMA)
        USING gen_random_uuid()::text, NEW.id, NEW.queue, NEW.attempts,
              NEW.status, '{}'::jsonb, '{}'::jsonb;
      END IF;
      RETURN NEW;
    END;
    $$
    """,
    "DROP TRIGGER IF EXISTS dewey_task_event_transition ON task_entries",
    "CREATE TRIGGER dewey_task_event_transition AFTER UPDATE OF status ON task_entries FOR EACH ROW EXECUTE FUNCTION dewey_capture_task_event()",
)


def install_task_history(connection):
    """SQLAlchemy migration helper, after the two history tables exist.

    Caller owns the migration transaction. PostgreSQL only; create_all on a fresh
    schema also installs this via metadata DDL. Existing tables need this explicit
    step, because create_all is not a migration mechanism.
    """
    if connection.dialect.name != "postgresql":
        raise ValueError("Task history requires PostgreSQL")
    from sqlalchemy import DDL

    for statement in HISTORY_SQL:
        # DDL compilation handles pyformat/asyncpg escaping; raw driver SQL
        # would interpret PostgreSQL format('%I', ...) as a bind placeholder.
        connection.execute(DDL(statement.replace("%", "%%")))
