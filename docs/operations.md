# Operations

Everything an operator needs is one command and one small API underneath it. Both
answer from the ledger: there is no second system to ask.

## The `dewey` command

```bash
dewey --app myapp.worker:create_application <command>     # or set DEWEY_APP
python manage.py dewey <command>                          # Django: no factory needed
```

The command loads the same application factory the worker uses, so it sees the same
database, handlers and schedules. Add `--json` to any command for machine-readable
output.

| Command | What it does |
|---|---|
| `run [--queues a,b] [--concurrency N]` | Run a worker until stopped. See [the worker](native-worker.md). |
| `queues` | Counts by status for every queue. |
| `tasks list [--status S] [--queue Q] [--type T] [--limit N]` | Tasks, newest first. |
| `tasks show ID` | One task with its history. |
| `tasks retry ID` | A failed or dead task back to pending, with a fresh attempt budget. |
| `tasks kill ID` | Stop retrying; the task becomes dead. A running attempt loses ownership. |
| `dlq list` / `dlq retry ID` | Dead letters, and putting one back. |
| `workers` | Workers with a fresh heartbeat, and the queues they serve. |
| `schedules list` / `plan` / `apply` / `pause` / `resume` | See [schedules](schedules.md). |
| `doctor [--queues a,b]` | Readiness check. |

Exit statuses: `0` success; `1` doctor found errors, or the task does not exist; `2` a
usage error, or `schedules plan` found changes to apply.

Task arguments and error text are **hidden by default**: they can hold personal data,
and listings end up in terminals, tickets and chat. `--reveal` on `tasks list`,
`tasks show` and `dlq list` includes them.

```text
$ dewey queues
QUEUE    PENDING  DISPATCHING  PROCESSING  COMPLETED  FAILED  DEAD  EXPIRED
default  3        0            2           4891       1       1     0
reports  0        0            0           212        0       0     0
```

## Doctor

`dewey doctor` reads the ledger and this process's registries and reports findings.
It exits non-zero on any error, so it works as a deploy gate and as a monitor.

| Finding | Means |
|---|---|
| `schema` | A Dewey table or column is missing. Apply migrations. |
| `no_worker` | No worker has written a heartbeat in the last 45 seconds. |
| `queue_unserved` | A queue with unfinished work (or one you named with `--queues`) has no running worker. |
| `unknown_task_type` | Waiting tasks have no registered handler in this code. Workers leave them unclaimed. |
| `unrecovered_work` | A task holds a lease that expired minutes ago and nothing is recovering it. |
| `schedule_drift` | Declared schedules differ from the applied ones. Run `dewey schedules apply`. |
| `schedule_unknown_task` | An applied schedule creates a task type with no handler here. |
| `schedule_late` | A schedule has been due for minutes and nothing is creating its ticks. |
| `schedule_paused` | (warning) A schedule is paused. |

Run it from the application's own code and environment: "no registered handler" is
judged against the handlers *this* process imported.

## Alerts

Looking is not enough for work that fails at night. Register one hook and route the
events wherever you already look:

```python
import dewey

@dewey.on_alert
def notify(event: dict) -> None:
    if event["event"] == "task_dead":
        error_tracker.capture_message(f"Task {event['task_type']} dead-lettered", extra=event)
```

or by path, in the worker settings (`alert_hook="myapp.alerts:notify"`, Django:
`DEWEY["ALERT_HOOK"]`).

| Event | Fields | Raised when |
|---|---|---|
| `task_dead` | `task_id`, `task_type`, `queue`, `attempts`, `reason` | A task used up its attempts, failed permanently, was cancelled, or was killed with `dewey tasks kill`. |
| `execution_recovered` | `task_id`, `task_type`, `queue`, `reason`, `status` | A worker died or a task hit its hard timeout, and the task was recovered. |
| `schedule_tick_missed` | `schedule`, `missed` | Ticks came due and were not run (an outage, a pause, or a `skip` schedule running late). |
| `worker_heartbeat_stale` | `message` | `dewey doctor` found no running worker for a queue. Run doctor from a monitor to get this one. |

The hook runs in the process that made the change, after the transaction commits, so
an alert never describes something that was rolled back, and after the
[failed-attempt log record](logging.md#failed-attempts) for the same change has gone
out. Keep it quick. If it raises, the exception is logged and nothing else happens: an
alert cannot change what the ledger did. It runs in a copy of the current context:
it sees the task's trace and current span (an error tracker or `logfire.error` call
inside it lands under the attempt), and anything it attaches to that context, a
span it forgets to end or an instrumentation-suppression flag, ends with the call
instead of silencing the worker's own records. There is no vendor code in Dewey;
the hook is yours.

## The operations API

The command is a thin layer over `dewey.ops`. Every operation has a name, takes keyword
arguments and returns plain JSON-safe data, so an admin page, a chat command or an
MCP server can be another thin layer over the same thing.

```python
from dewey.sqlalchemy.ops import run       # run(session, name, **arguments)
from dewey.django.ops import run           # run(using, name, **arguments)

with Session(engine) as session, session.begin():
    dead = run(session, "tasks.list", status="dead", limit=20)
    run(session, "tasks.retry", task_id=dead[0]["id"])
```

With an `AsyncSession`: `await session.run_sync(run, "queues")`. The caller owns the
transaction; with Django, call inside `transaction.atomic()`.

| Operation | Arguments | Returns |
|---|---|---|
| `queues` | | `[{queue, pending, dispatching, processing, completed, failed, dead, expired}]` |
| `tasks.list` | `status`, `queue`, `task_type`, `limit` (1-500), `reveal` | task summaries, newest first |
| `tasks.show` | `task_id`, `reveal` | a task summary plus `history`, or `None` |
| `tasks.retry` | `task_id` | the task summary, or `None` |
| `tasks.kill` | `task_id` | the task summary, or `None` |
| `workers` | | `[{instance_id, backend, database, queues, started_at, last_seen_at, dewey_version}]` |
| `schedules.list` | | one entry per applied schedule |
| `schedules.plan` | | `{added, changed, removed: [{name, unstarted_ticks}], unchanged}` |
| `schedules.apply` | `keep_pending` | the same shape, after applying |
| `schedules.pause` | `names` (None: all), `seconds` (None: until resumed) | the names paused |
| `schedules.resume` | `names` (None: all) | the names resumed |
| `doctor` | `queues` | `{ok, findings: [{id, level, message}]}` |

A task summary is `{id, task_type, queue, status, priority, attempts, max_attempts,
schedule, created_at, scheduled_for, started_at, completed_at, expires_at, has_error}`,
with `args`, `kwargs` and `error` added when `reveal` is true. Timestamps are ISO 8601.

For anything this does not cover, the ledger is a table: see the
[query API](query-api.md) and plain SQL.

## The admin page (experimental)

Django projects can also mount the [admin overview](dashboard.md). It is optional and
experimental: the command and the API above are the supported surface.
