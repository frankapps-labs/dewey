# Upgrading from 0.5 to 0.6

0.6 changes Dewey's shape. Before, Dewey kept the ledger and handed task IDs to a
broker; a dispatcher process did the handing. Now Dewey also owns the runner and the
clock: **one process type, `dewey run`, claims, executes, recovers and creates schedule
ticks.** There is no broker and no dispatcher.

## What was removed

- The Huey adapter, `dewey.adapters`, `dewey.contrib.django_huey` and the `huey` extra.
- The dispatcher: `dewey.dispatcher`, the dispatch backends, the `dewey_dispatcher`
  command and LISTEN/NOTIFY wake-ups.
- The sweep functions (`sweep`, `sweep_failed`, `sweep_stuck`, ...). Workers recover
  their own leases.
- `manage.py dewey_doctor`; use `manage.py dewey doctor`.
- Django settings `DISPATCH`, `BATCH_SIZE`, `IDLE_POLL_SECONDS`, `SWEEP_*`,
  `DISPATCH_TIMEOUT_SECONDS`, `STUCK_THRESHOLD_MINUTES` and `WORKER_DATABASE`. An old
  key is now an error, not a silent no-op.

Unchanged: `@dewey.task`, `create_task`, `create_or_get_task`, policies, the query
functions, and your handlers.

## Steps

1. **Stop the old processes**: the dispatcher and the broker workers. Let in-flight
   tasks finish first if you can.
2. **Upgrade and migrate.**
   - Django: `python manage.py migrate`. Migration `0008` hands back any task the old
     path left claimed, `0009` adds the schedule table, and `0010` adds the index
     workers claim from (built concurrently, so writes continue meanwhile).
   - SQLAlchemy: in a migration, add the columns listed in
     [execution ownership](execution-ownership.md) if your `task_entries` predates
     them, then create the tables 0.6 adds (schedules, task history and its state),
     install the history trigger, add the claim index, and run the same hand-back
     once.

     ```python
     from dewey.dashboard.history import install_task_history
     from dewey.sqlalchemy.models import Base

     with engine.begin() as connection:
         Base.metadata.create_all(connection)  # creates missing tables; alters nothing
         install_task_history(connection)  # safe to repeat
     ```

     ```sql
     -- What a worker claims, in the order it claims it. Run outside a transaction.
     CREATE INDEX CONCURRENTLY ix_task_claim ON task_entries
         (queue, priority DESC, coalesce(scheduled_for, created_at), created_at, id)
         WHERE status IN ('pending', 'failed') AND dispatch_token IS NULL;
     ```

     ```python
     from dewey.sqlalchemy.upgrade import reset_legacy_claims

     with Session(engine) as session, session.begin():
         reset_legacy_claims(session)
     ```

     A worker refuses to start, and `dewey doctor` reports why, until all of this
     is in place: it checks the columns, the three tables and the trigger.
3. **Remove the broker wiring** from your code and settings: the adapter, the
   `dispatch` callable, the `DEWEY["DISPATCH"]` setting.
4. **Start workers**: `dewey run --app myapp.worker:create_application`, or
   `python manage.py dewey run`. See [getting started](getting-started.md).
5. **Check**: `dewey doctor`.

## Periodic work

If a cron or a broker's periodic task was calling `create_task` for you, it can keep
doing so. To move it into Dewey, declare a [schedule](schedules.md) and run
`dewey schedules apply`. Both can run side by side while you switch over: give the
external trigger the same idempotency key Dewey uses (`schedule:<name>:<tick>`) or
simply accept one duplicate run on the day you switch.

## Behaviour worth knowing

- **Workers claim only task types they have handlers for.** A task whose handler is
  not deployed yet now waits, instead of failing an attempt.
- **Hard timeouts exist.** A task that runs past its limit (an hour by default, or its
  own `execution_timeout_s`) is stopped and retried. Handlers can read
  `dewey.current_task().remaining()`.
- **Inline `process_task` is a test helper**, not a production path. Nothing recovers
  a row it leaves behind. It binds `dewey.current_task()` and applies the policy's
  `statement_timeout_s`/`lock_timeout_s` the way the worker does, so a handler
  tested inline runs under its production budgets.
- **`create_task` returns a record, not a model instance.** On Django it is a
  `dewey.core.types.TaskEntry` dataclass: use `.id`, not `.pk`, and load the model
  with `TaskEntry.objects.get(id=task.id)` if you need it.
- **Task history is recorded by a database trigger.** Each time a task fails,
  completes, dies or expires, one small row is added to `dewey_task_events` in the
  same transaction; `dewey tasks show` reads them. They are deleted with their task.
- **A Django project with `django.contrib.admin` installed gets a Dewey page in it**,
  registered automatically. It is experimental: see the [admin overview](dashboard.md)
  for its permissions. Completed-task retention is off until you configure it.
  There is no setting to opt out. A project that registered its own `ModelAdmin`
  for `TaskEntry` gets `AlreadyRegistered` from whichever registration runs
  second: list `dewey.django` before that app in `INSTALLED_APPS` and call
  `admin.site.unregister(TaskEntry)` in the app's `admin.py` before registering
  your own.
