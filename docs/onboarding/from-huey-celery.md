# Coming from Huey or Celery

You do not have to move everything at once, and you should not. Dewey rides on the same
broker your workers already consume, so a Dewey-managed task and a plain `@huey.task` can
live in one worker process indefinitely. Migrate one task type per pull request.

## What actually changes

Two things:

1. **The call site.** `f.delay(42)` becomes `create_task(task_type="f", args=[42])`.
   Producers stop importing handler modules.
2. **Retry mechanics leave the handler.** `self.retry(...)`, manual delay lists and
   `time.sleep` come out; the handler raises and policy decides.

The handler *body* usually does not change at all. Both Huey and Celery consumers already
write id-and-look-up handlers, which is exactly Dewey's shape.

## Pattern by pattern

| Huey / Celery | Dewey |
|---|---|
| `@task() def f(event_id): ...` | `@dewey.task("f") def f(event_id): ...` — body unchanged |
| `f.delay(42)` | `create_task(task_type="f", args=[42])` |
| `f.delay(client_id=42)` | `create_task(task_type="f", kwargs={"client_id": 42})` |
| `f.schedule(args=[42], delay=60)` | `create_task(task_type="f", args=[42], scheduled_for=now + timedelta(seconds=60))` |
| `transaction.on_commit(lambda: f.delay(42))` | `create_task(task_type="f", args=[42])` inside the same `atomic()` — the wake-up is already transactional |
| `RETRY_DELAYS = [3, 3, 3]` walked by hand | `@dewey.task("f", max_attempts=3, backoff=dewey.Constant(3))` |
| `@app.task(bind=True)` + `raise self.retry(countdown=120)` | `raise dewey.TransientError(...)` with `backoff=dewey.Exponential(base_s=120)` |
| `raise self.retry(countdown=e.retry_after)` | `raise dewey.RetryAfter(e.retry_after)` |
| `@app.task(base=QueueOnce)` / a Redis dedupe key | `create_task(..., idempotency_key=f"command:{id}")` |
| `@periodic_task(crontab(...))` | Keep cron as the trigger; the body calls `create_task` |
| `@app.task(autoretry_for=(ConnectionError,))` | `@dewey.task("f", retry_on=(ConnectionError,))` |
| `@app.task(throws=(NotFound,))` | `@dewey.task("f", fail_fast_on=(NotFound,))` |
| Broker-level `retries=N` | Delete it. Dewey owns the attempt budget; two retry engines run work twice |

## A worked example

Before, with Huey and a hand-rolled retry ladder:

```python
RETRY_DELAYS = [3, 3, 3]

@huey.task()
def send_event(event_id, delay_queue=None):
    event = Event.objects.get(id=event_id)
    try:
        broadcaster.send(event)
    except WebsocketError:
        delays = delay_queue if delay_queue is not None else list(RETRY_DELAYS)
        if delays:
            delay = delays.pop(0)
            send_event.schedule(args=[event_id, delays], delay=delay)

@receiver(post_save, sender=Event)
def event_saved(sender, instance, created, **kwargs):
    if created:
        transaction.on_commit(lambda: send_event(instance.id))
```

After:

```python
import dewey

@dewey.task(
    "events.send",
    max_attempts=4,
    backoff=dewey.Constant(3),
    retry_on=(WebsocketError,),
    fail_fast_on=(Event.DoesNotExist,),
)
def send_event(event_id):
    event = Event.objects.get(id=event_id)
    broadcaster.send(event)          # let it raise; Dewey reschedules

@receiver(post_save, sender=Event)
def event_saved(sender, instance, created, **kwargs):
    if created:
        create_task(task_type="events.send", args=[instance.id])
```

The retry ladder, the `delay_queue` threading and the `on_commit` wrapper are all gone.
What replaces them is queryable: every attempt, its error and its next scheduled time are
columns on a row.

## Running side by side

One broker, one worker process, both kinds of task. On Django,
`dewey.contrib.django_huey` registers Dewey's processor on the same
`huey.contrib.djhuey.HUEY` your existing tasks already use, so `manage.py run_huey`
serves both:

```python
# settings.py
DEWEY = {"DISPATCH": "dewey.contrib.django_huey.dispatch"}
```

```python
# myapp/tasks.py
from huey.contrib.djhuey import db_task
from dewey.contrib.django_huey import adapter, dispatch  # noqa: F401

# Not yet migrated: still enqueued directly, invisible to Dewey
@db_task()
def legacy_thing(x):
    ...
```

If you wire Huey yourself instead, keep two things the contrib module would have done
for you: wrap the processor in `huey.contrib.djhuey.close_db` (so worker threads do not
hold one Django connection forever), and export a module-level `dispatch` wrapper,
because `DEWEY["DISPATCH"]` is imported with `import_string` and cannot resolve object
traversal like `"myapp.tasks.adapter.dispatch"`:

```python
# myapp/tasks.py
from huey.contrib.djhuey import HUEY, close_db
from dewey.adapters.huey import HueyAdapter
from dewey.django import process_task

adapter = HueyAdapter(HUEY)
adapter.register(close_db(process_task))

def dispatch(task_id: str):
    return adapter.dispatch(task_id)   # DEWEY = {"DISPATCH": "myapp.tasks.dispatch"}
```

The consumer serves both. The dispatcher only ever sees Dewey rows. Cut over one task
type at a time; there is no flag day.

## Things that will trip you up

**`task_type` is a string, checked at dispatch time.** A typo in `create_task` will not
fail at import. It fails as a task that retries with `No handler registered for task type
'evnts.send'` in `error` and eventually dead-letters — visible, but only once it runs.
Keep the names in one module and reference them, rather than typing literals at call sites.

**Arguments must survive JSON.** No pickled objects, no ORM instances, no bytes. If you
were relying on Celery's pickle serializer, that ends here: pass IDs. `datetime`, `UUID`,
`Decimal` and `Enum` are converted to strings, so a handler annotated
`def f(when: datetime)` receives a `str` and should parse it.

**Handlers must not sleep or retry themselves.** No `time.sleep`, no `self.retry`. Raise
and exit; Dewey reschedules on a fresh worker. This is what keeps handlers unit-testable
without mocking a framework.

**`create_task` replaces `on_commit` only inside the same transaction, on the same
database alias.** That is the upgrade: one commit or one rollback for the business row
and the task together. It holds on exactly one alias. A task written through a
different alias — via a database router or an explicit `using=` — rides a separate
connection and a separate transaction, *even when both aliases point at the same
physical Postgres*, and it will commit while the business write beside it rolls back.
Producers keep writing on the business alias; reserve dedicated aliases for the
dispatcher (`DEWEY["DATABASE"]`) and worker (`DEWEY["WORKER_DATABASE"]`), which run
after commit anyway.

**A dispatcher has to be running.** It is a new process beside the worker. Due retries are
claimed directly and wake pacing follows the earliest due row; the dispatcher's periodic
sweep remains the recovery path for abandoned dispatching/processing work.

**There is no `.delay()`.** Deliberately: it is what re-couples producers to worker
imports. If you want type safety at the call site, wrap it yourself —

```python
def enqueue_send_event(event_id: int) -> None:
    create_task(task_type="events.send", args=[event_id])
```

**Periodic tasks still need an external trigger.** Dewey has no scheduler in this release.
Keep `@periodic_task` or beat, and have the body call `create_task`:

```python
@huey.periodic_task(crontab(minute="*"))
def _scan_for_work():
    for event in Event.objects.pending():
        create_task(task_type="events.send", args=[event.id])
```

## Suggested order

1. Deploy the dispatcher with nothing pointed at it. Confirm it runs and idles quietly.
2. Migrate one low-stakes task type. Watch `get_stats()` and the `dead` count.
3. Migrate the rest in whatever order suits you, one PR each.
4. Delete broker-level `retries` settings as each type moves over.
