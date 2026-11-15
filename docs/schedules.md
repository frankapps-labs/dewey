# Schedules

A schedule turns time into ordinary task rows: **one row per tick**. From that row on,
a scheduled run is a task like any other: claimed by a worker, retried, dead-lettered,
recovered if its worker dies, and visible with its history.

There is no scheduler process to keep alive. Every worker checks for due ticks as part
of its normal loop, and creating a tick and advancing the schedule happen in one
transaction, so any number of workers produce exactly one row per tick.

## Declaring

```python
import dewey

# Next to the handler: a schedule named after the task type.
@dewey.task("reports.daily", schedule=dewey.cron("15 3 * * *", tz="Europe/London"))
def daily_report() -> None:
    ...

# Or on its own, when the task takes arguments or needs other policies.
dewey.schedule(
    "poll-inventory-eu",
    task="inventory.poll",
    when=dewey.every(minutes=1),
    kwargs={"region": "eu"},
)
```

A schedule's identity is its **name**, never the handler's import path. Renaming or
moving the function changes nothing; renaming the schedule is a removal plus an
addition.

`dewey.cron(expression, tz="UTC")` takes the standard five fields (minute, hour, day of
month, month, day of week) with lists, ranges, steps and month/weekday names
(`0 9 * * mon-fri`), plus `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly`.
When both day fields are restricted, either one matching is enough, as in classic
cron. Seconds and the `L`/`W`/`#` extensions are not supported.

`dewey.every(minutes=5)` is a fixed interval of at least one minute, aligned to the
Unix epoch, so every process agrees on the instants without coordination.

## Applying: plan, then apply

Code is the declaration; a table holds the applied state. Workers never write
definitions, so old and new code running side by side during a deploy cannot fight
over them. One command does:

```bash
dewey schedules plan     # what would change; exits 2 if anything would
dewey schedules apply    # make the table match the code
```

```text
$ dewey schedules plan
add: poll-inventory-eu
change: reports.daily
remove: legacy-export (3 unstarted)
unchanged: billing.hourly
```

Run `apply` in your release step, after migrations. `plan` is safe anywhere: run it
locally before you push, or in CI as a gate that fails a pull request which changes
schedules without saying so.

What `apply` does:

| Change | Effect |
|---|---|
| Added | Starts from now. Nothing is backfilled. |
| Changed | New definition, same cursor and same paused state. |
| Removed | Deleted, and its ticks that have not started expire, including one a worker has claimed but not begun. `--keep-pending` leaves them to run. Ticks already running finish. |

A worker whose code declares something the table does not have logs it at startup, and
`dewey doctor` fails until you apply. On a single-node install, where exactly one
version of the code runs at a time, set `auto_apply_schedules` and the worker applies
at start instead.

## Choosing the policies

Three questions decide a schedule's behaviour. The defaults suit most periodic work.

**1. What should happen to ticks that came due while nothing was running?** (`catch_up`)

| Value | After an outage | Use for |
|---|---|---|
| `"once"` (default) | Runs the latest missed tick; the rest are recorded as missed | Work that brings things up to date: a clean-up, a sync, a cache refresh |
| `"skip"` | Runs nothing that is more than a minute late | Work that is only meaningful on time: a reminder for a moment that has passed |
| `"all"` | Runs every missed tick, oldest first, keeping the most recent 100 | Work where each period is its own unit: a billing run, an hourly export |

**2. What if the previous run has not finished when the next tick is due?** (`overlap`)

`"skip"` (default) creates no new tick while an earlier one of the same schedule is
unfinished, including one that failed and is waiting to retry. `"allow"` creates it
regardless.

**3. Is a late tick still worth running?** (`expire`)

`"next_tick"` (default) lets a tick that has not started by the time the next one is
due expire instead of running late. `None` keeps it until it runs.

`catch_up="all"` means every period matters, so it implies `overlap="allow"` and
`expire=None`.

### Worked examples

**A one-minute poll of an external system.**

```python
dewey.schedule("poll-inventory", task="inventory.poll", when=dewey.every(minutes=1))
```

The defaults are right. If a poll takes three minutes, the next two ticks are skipped
rather than stacked behind it. If the worker was down for two hours, one poll runs when
it returns, not 120. If a poll fails, its retry is only worth running until the next
minute: after that it expires and the next tick does the work. For a poll you usually
also want `max_attempts=1` on the task, since the schedule itself is the retry.

**A daily clean-up.**

```python
@dewey.task("sessions.purge", schedule=dewey.cron("15 3 * * *", tz="Europe/London"))
def purge_expired_sessions() -> None: ...
```

Also the defaults. If the database was unreachable at 03:15, the purge runs once when
it comes back, late but done. A purge that fails retries on the task's own backoff
until the next day's tick replaces it.

**An hourly billing run.**

```python
dewey.schedule(
    "billing-hourly", task="billing.run", when=dewey.cron("0 * * * *"), catch_up="all"
)
```

Each hour is a separate obligation, so after an outage every missed hour runs. The
handler needs to know *which* hour it is for; that is the tick, not "now":

```python
from dewey.django.models import TaskEntry

@dewey.task("billing.run")
def run_billing() -> None:
    task = TaskEntry.objects.get(id=dewey.current_task().task_id)
    period_start = datetime.fromisoformat(task.metadata["tick"])
    ...
```

Every tick's task row carries `metadata["schedule"]` and `metadata["tick"]` (the
nominal instant, UTC), and the idempotency key `schedule:<name>:<tick>`.

### When not to schedule

If something in your system knows the moment work is needed, create the task there,
in that transaction, with a priority:

```python
with transaction.atomic():
    code = LoginCode.objects.create(user=user)
    create_task(task_type="login.send_code", args=[str(code.id)], priority=10)
```

A one-time code, a confirmation email or a webhook reaction should not wait for a
poll to notice it. Schedule only when the trigger is time itself (03:15 every day) or
an external system that cannot call you.

## Time zones and daylight saving

Schedules are evaluated on the wall clock of their time zone and stored as UTC
instants. All "is it due?" decisions use the database clock, so workers on hosts with
different or drifting clocks agree.

On the two days a year the clocks change:

- **A schedule that names its hours** (`30 1 * * *`) is anchored to the wall clock. If
  01:30 does not exist that day, it fires once at the first valid instant after the
  gap. If 01:30 happens twice, it fires once, the first time.
- **A schedule that runs every hour** (`*/10 * * * *`, `0 * * * *`) follows elapsed
  time. It keeps its cadence through both changes: no burst, no silent hour.

## Pausing

```bash
dewey schedules pause poll-inventory              # until resumed
dewey schedules pause --all --for 10m             # ends by itself
dewey schedules resume poll-inventory
```

Paused is operator state: `apply` never changes it. Ticks that come due during a pause
are handled by each schedule's catch-up policy when it resumes.

`pause --all --for 10m` before a deploy holds the clock while old and new code run
together, and cannot leave it stopped if the deploy fails. Most schedules do not need
it: a tick created mid-deploy is an ordinary task and runs on whichever version claims
it.

## Seeing what happened

```bash
dewey schedules list
dewey tasks list --type inventory.poll
dewey doctor
```

`schedules list` shows each schedule's next due time, last fire, and how many ticks
were missed or skipped. `doctor` fails when declared and applied schedules differ,
when a schedule creates a task type no handler is registered for, and when a schedule
has been due for minutes with nothing creating its ticks. A missed tick also raises a
`schedule_tick_missed` [alert](operations.md#alerts).

A large catch-up is worked off 25 ticks per pass, a few seconds apart, so it never
holds one long transaction.

## Volume

A one-minute schedule is 1,440 rows a day. Completed rows are kept until you remove
them; see [retention](dashboard.md) or `purge_completed` in the
[query API](query-api.md), and run it on a schedule of its own.

## Other runtimes

Nothing about schedules is specific to the Python worker. A tick is an ordinary task
row, the stored definition carries everything needed to create one, and hosts that
serve non-Python runners call `tick()` on the [pull-runner facade](pull-runners.md).
