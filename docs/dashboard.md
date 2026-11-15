# Admin overview (experimental)

The supported operator surface is the [`dewey` command and operations API](operations.md).
This Django admin page is optional and experimental: its layout, permissions and JSON
may change between minor releases.

**Overview** shows creation-window counts, configured queues, active-owner
observations and terminal-transition graphs; **Tasks** shows the ledger, diagnostics
and event history. The Django adapter also provides **Retention** configuration.
**Dead letters** is a `status=dead` preset, not another queue. Manual refresh only.
The overview keeps contextual count labels and collapses longer explanations under
**About these metrics**. Active task claims are not a worker-health panel.
The only task command is an explicitly approved **single Retry**; no HTTP purge,
bulk retry, kill, payload editing, Slack or automatic classifier.

## Recovery-focused task view

Task detail leads with a read-only explanation of the observed ledger state and
what must happen next. Django supplies additive `task.recovery`, `task.expires_at`
and response `observed_at` fields. Older portable hosts without these fields show
an explicit unavailable explanation, not an inferred recovery promise.

Elapsed ownership leases mean **awaiting recovery**, not proof of a crash or a
healthy replacement worker. Scheduling, expiry, attempt budget and ownership are
shown separately from business approval and business success. The attempt timeline
shows only recorded outcomes and manual retry events, oldest first within the
latest 25 events; it does not invent starts, crash causes or missing history.
Technical details and diagnostics are expandable. Refresh remains manual.

Hosts may mount with `{initialTab: "tasks", readOnly: true, navigation: true}`
(or `data-initial-tab="tasks"`, `data-read-only="true"`, `data-navigation="true"`).
Read-only presentation omits Retry and Retention controls; **the host must still
deny writes at its API**. Optional navigation reserves `dw_tab`/`dw_task` URL query
parameters for selection, reload and Back/Forward; enable for one widget per page.
Task IDs in URLs must be appropriate for host browser-history policies. Filters
remain local to the current mount. No domain workflow association is inferred.

## Django admin and access

Install `dewey[django]`, add `dewey.django` and standard admin/auth apps, migrate,
and mount `admin.site.urls`. Open **Dewey → Task entries**, or reverse
`admin:dewey_taskentry_changelist`. No frontend build or worker startup required.
For another AdminSite register `dewey.django.admin.TaskEntryAdmin` there.

All routes require active staff and `dewey.view_taskentry`, with private no-cache
responses. Observation/asset routes are GET-only; inherited CRUD routes are absent.
Retry additionally requires **`dewey.retry_taskentry`**, a CSRF-protected POST,
explicit confirmation, current event/attempt and a nonempty audit reason≤500 chars.
The command reevaluates eligibility under a row lock; stale/duplicate requests
reject without another budget grant or audit. Superusers still need host policy
approval. Model change permission does not grant retry.

Reads use `router.db_for_read(TaskEntry)` unless `DEWEY_DASHBOARD_DATABASE_ALIAS`
is set; retry/reporting use the write router or that explicit alias. Replica reads
can be stale; the command always rereads the writable database. Configure a
consistent alias for operators who need immediate read-after-write visibility.
The dashboard does not migrate schema, start workers or execute handlers.

For local previews or multiple applications on one host, give each host application
unique Django `SESSION_COOKIE_NAME` and `CSRF_COOKIE_NAME` values. Cookies are
scoped to domains/paths, **not ports**: another localhost application using the
same default names can replace your session. The library must not rename the
embedding application's cookies. Sign-in redirects are reported as an ended
session, not a task-data failure.

## Queues and task priority

Set `DEWEY_DASHBOARD_QUEUES = ("default", "reports")` from the application's
**authoritative queue configuration**. Names must be nonempty strings≤50 chars.
Configured names remain visible at zero counts; observed names are also included
and labelled observed-only. Combined names sort alphabetically, capped50 with a
truncation indicator. Catalog entries are not worker health/capacity observations.

Queue order has no scheduling effect. Task priority is shown on the ledger;
native claiming still uses task priority descending then effective schedule/time.
The ledger itself remains newest-first, not next-to-execute order.

## Diagnostics, privacy and host policies

Arguments, kwargs, metadata, raw errors, idempotency/effect keys and fencing tokens
are never sent to the browser. Without reviewed diagnostics, show local task,
attempt and event correlation with remote references **unavailable**. An older
attempt's Sentry/Logfire reference remains attached to its historical event in
the detail response, never as the current attempt's reference.

Hosts explicitly enrich a **committed failure event**, not arbitrary error text:

```python
from dewey.dashboard.operations import Diagnostics, FailureCategory
from dewey.django.operations import record_diagnostics

record_diagnostics(
    event_id, attempt,
    Diagnostics(
        category=FailureCategory.TRANSIENT,
        summary="Reviewed temporary provider outage",
        business_label="Batch record",
        business_url="/admin/orders/batch/123/change/",
        provider="sentry", reference="reviewed-event-id",
        reference_url="https://sentry.example.invalid/issues/123/",
    ),
)
```

`event_id` is the exact persisted failed/dead event, and `attempt` must match.
Reporting happens **after outcome commit**. Catch reporting failures separately;
missing/failed reporting must not change the delivery outcome. No SDK is installed
or automatically invoked. Hard timeout/crash can bypass application reporting;
the database still records the parent-observed terminal transition without a
fabricated provider reference.

Set `DEWEY_DASHBOARD_REFERENCE_ORIGINS = ("https://sentry.example.invalid",)` for
explicit trusted HTTPS origins. Reference links reject userinfo, queries,
fragments, percent encodings, whitespace/control characters and backslashes.
Business links must be plain local absolute paths, never protocol-relative URLs.
Hosts own reviewed non-sensitive labels, summaries≤500, reference IDs≤200, paths,
and origin allowlists. These validators are **not arbitrary-text anonymizers**.
Allowed providers: `sentry`/`logfire`; categories: typed `unknown`, `transient`,
`business`, `validation`. Invalid persisted diagnostic JSON is hidden.
The older `DEWEY_DASHBOARD_ERROR_SUMMARY(task)` safe-summary hook remains optional;
its output alone does not classify failure or authorize retry.

### Diagnostics-first single Retry

Set `DEWEY_DASHBOARD_RETRY_POLICY` to a local, bounded, side-effect-free callable
`(task, event) -> RetryDecision`. Approval explicitly means:

1. The diagnosed transient condition has recovered.
2. The business obligation is still eligible now.
3. Replay on the **same operation keys** is safe, including provider receipt
   retention and any possibly committed effects.

```python
from dewey.dashboard.operations import RetryDecision

# In application code; implement these checks against your real domain/receipts.
def retry_policy(task, event):
    if task.task_type not in REPLAY_SAFE_TYPES:
        return RetryDecision(False, "No reviewed replay policy for this task type.")
    if not business_obligation_can_safely_replay(task.id):
        return RetryDecision(False, "Business eligibility or replay safety not established.")
    return RetryDecision(True, "Recovered; current obligation and receipt safety reviewed.")
```

Default is deny; exceptions/wrong return types deny. **Transient is not synonymous
with safe**: a network timeout can follow a committed business effect. No error
string matching or changes to automatic retry policy are made. Do not call remote
services while holding the task lock.

The command requires DEAD, a matching current DEAD event/attempt, transient
reviewed diagnostics, **absent** native/execution ownership, and unexpired absolute
expiry. It preserves task ID, args/kwargs, attempts, priority, queue, expiry,
idempotency key and failure history. It sets `max_attempts = attempts + 1`,
`status=pending` and schedule due now; **one additional attempt**, not a counter
reset or cloned task. Every subsequent manual retry needs another fresh diagnosed
DEAD event and explicit approval. Original task-scoped effect keys remain stable.

The same transaction records `retry_requested`: actor ID, time, safe reason,
source event ID and old/new budget, then queues wake-on-commit. Retry is not undo,
cancellation or an exactly-once promise. Existing programmatic `retry_task`/bulk
helpers retain their old reset semantics; the dashboard does **not** call them.

## Event history and truthful graphs

Django migration **0005_task_history** installs event/state tables and a PostgreSQL
status-transition trigger. Every actual transition into failed/completed/dead/
expired records database clock, task, queue, attempt and kind in the **same
transaction**. Native sync/async/Django, inline executors, bulk and direct
SQL writers share this capture. Same-state updates and rolled-back transitions
create no event. No raw errors, payloads, ownership or effect keys are copied.

The trigger captures UPDATE transitions only, not historical/imported rows inserted
already terminal. **No legacy backfill** from created_at/updated_at is attempted.
The explicit installation timestamp is exposed; pre-installation buckets are
unavailable (null), and boundary buckets are partial. Counts are recorded
**Completed** and **Entered DEAD** events, not unique-task creation cohorts,
business delivery, latency or an SLA. Reentry into DEAD after retry counts again.

Fixed UTC buckets: last1h=5min,24h=1h,7d=6h; at most13/25/29 buckets, optional exact
queue filter. Graphs render on page load, Refresh or user filter changes, never
polling. A combined, host-themed outcome **line chart** defaults to cumulative
recorded transitions since available history in the selected window. Switch to
per-bucket counts locally without a new query. Both lines share a count axis;
labelled time/count axes, hover values, legend toggles, fullscreen and accessible
bucket values are provided. Unknown history remains null/gapped, not invented0.
Chart.js4.4.8 and bundled @kurkle/color0.3.2 are vendored with MIT notices, loaded
locally as a scoped ES module; no CDN or host-global Chart overwrite.
Creation-window summary cards remain explicitly separate from transition graphs.

History follows task retention: task deletion cascades events, so retained counts
can shrink. Latest25 per-task events are shown with truncation; full retention,
archive/export and large-history performance qualification are host responsibilities.
Do not disable the trigger or edit event identities/times to manufacture history.
Reporting may enrich diagnostics; transition identities/audits remain unchanged.

### SQLAlchemy schema adoption

Fresh `Base.metadata.create_all()` creates `TaskEventModel`/`HistoryStateModel` and
installs the PostgreSQL trigger. Existing databases require a reviewed explicit
migration creating these tables/indexes (event FK with `ON DELETE CASCADE`), then:

```python
from dewey.dashboard.history import install_task_history

# Within your migration's transaction, after table/index creation:
install_task_history(connection)
```

Use the same coordinated upgrade/drain discipline as
[execution ownership](execution-ownership.md). Worker startup never creates schema.
The writing DB role needs INSERT on `dewey_task_events`; the dashboard/reporting
role needs the relevant SELECT/INSERT/UPDATE privileges. Trigger installation is a
migration privilege, not an operator UI capability. Non-PostgreSQL deployments
have no recorded history or dashboard retry. Indexes on event time, queue+time and
task+time support bounded projections; output bounds do not guarantee SQL cost on
arbitrary production history. Qualify intended volume before enabling operations.

## Completed-task retention (Django)

Apply **0006_task_retention**. Retention is one policy for the selected writable
Dewey database, **not** the dashboard's queue/task-type/time filter. Default:
**30 days, disabled**. GETs never create policy/audit/run rows. The panel includes
selected-days cutoff/eligible-count preview (capped1000, marked truncated), latest
cleanup receipt and latest20 configuration changes. Previewing never saves or
deletes. Settings changes require staff/view access plus the distinct
**`dewey.manage_task_retention`** permission, CSRF-protected POST, explicit
confirmation, safe reason≤500 characters and current configuration revision.
Changes record actor ID, database time, old/new policy and revision atomically;
stale requests reject, unchanged settings do not add audits. Rejected saves show
an announced error and focus/scroll it into view; the saved policy remains unchanged. Retry/model-change
permissions do not grant retention configuration. No retention models have CRUD
admin routes.

### Approval and scheduling are different

Enabling permits a separately scheduled host job; it does **not** install cron,
start a worker, enqueue cleanup or delete during an HTTP request. The same local,
bounded, side-effect-free **`DEWEY_RETENTION_SAFETY(config, using)`** callback must
approve at enablement **and again for every batch**. Missing/wrong/raising callbacks
deny; superusers and enabled database flags cannot bypass it.

```python
from dewey.dashboard.retention import RetentionDecision

# Application-owned, reviewed for ALL task types in this database.
def retention_safety(config, using):
    return RetentionDecision(
        deduplication_safe=business_deduplication_survives_task_deletion(using),
        history_disposal_safe=old_history_is_archived_or_approved_for_disposal(using),
        minimum_days=30,
        explanation="Reviewed domain deduplication and history-disposal policy.",
    )
```

Those example business functions must be implemented/reviewed by the host; age
alone is not evidence. Deleting a task also deletes its creation-deduplication
key, so a later identical creation request may mint a **new task/effect key**.
Preserve business-level receipts/deduplication evidence beyond the replay window.
Event deletion can shrink retained chart counts; archive/roll up outside Dewey
or explicitly accept disposal (normally retain more than the7-day chart window).
No aggregate archive or dedupe tombstone is silently created.

### One bounded job

Invoke from the host's explicit scheduler/contained job runtime, using the same
**writable alias** as the policy; supply host time/resource bounds. For example:

```sh
python manage.py dewey_cleanup --database dewey --batch-size 100
```

Or call `dewey.django.retention.cleanup_completed_batch(using="dewey", batch_size=100)`
**outside an existing transaction**. This is one batch, not a loop/scheduler.
At most1000 tasks (default100) and1000 cascaded events; over-budget history blocks
without deletion, so reduce the host batch size or review the history separately.
Statements/locks have local2s/1s timeouts, not a universal hard job-time guarantee.
The job uses PostgreSQL READ COMMITTED, locks the policy, gets fresh DB time,
skips locked tasks and reevaluates completed status, old creation/completion,
absent execution/native ownership and **no event at/after the cutoff**. Equality,
unknown completion dates, active/failed/DEAD/expired rows are excluded.

Only the installed history FK cascade is allowed. Other inbound task references
(including business cascades) or application DELETE triggers block; schema locks
keep this check stable against concurrent DDL while allowing normal writers. Raw
SQL deletion does not run Django deletion signals. Business records are not a
cleanup target. The unmanaged `queries.purge_completed` helper retains its prior
semantics; it is **not** governed by this policy or used by the panel/job.

Start receipts persist independently. Successful deletion/count/finish are atomic;
failures retain a safe error code/run ID, never raw exceptions. Uncertain commit
acknowledgements do not overwrite an already-committed completion as failed.
A running receipt is **not liveness**: reconcile interrupted/unreachable outcomes
with host logs before inferring rollback. Disabling waits behind an in-flight
batch and prevents the next one, not undo/cancel of a committed batch. Audit/run
metadata survive task deletion and need their own eventual host retention.
A blocked/failed management command exits nonzero after printing its receipt;
disabled runs make no deletion. No schedule is shipped or enabled by default.

Use least-privilege roles: dashboard readers need SELECT; configuration writers
add INSERT/UPDATE on the policy and INSERT on its audit, not DELETE on tasks.
The job role separately needs receipt writes and task/event deletion privileges.
Keep schema privileges with the migration role. Retention persistence/job/admin
are Django-only; portable hosts may opt into the UI/protocol below, but no
SQLAlchemy retention controller/schema or FastAPI settings adapter is shipped.

## Portable assets and protocol

`dewey.dashboard` imports neither ORM. Hosts supply routes, authorization, reads
and optional command adapters; **no FastAPI adapter is shipped**.

| Route beneath same-origin API base | Contract-v1 response |
|---|---|
| GET `overview/` | `version`, `observed_at`, `hours`, `summary`, `queues`, truncation flags, `owners`, `limitations`, `throughput` |
| GET `tasks/` | `version`, `items`, `next_cursor` |
| GET `task/{id}/` | `version`, `task`;404 absent |
| POST `task/{id}/retry/` | `version`, audit `event_id`, `task_id`, `attempts`, `max_attempts`;409 rejected intent |
| GET `retention/` (opt-in) | `version`, saved `config`, `preview_days`, `cutoff_at`, bounded `eligible_count`, `safety`, `can_manage`, `last_run`, bounded `audits`, `limitations`; optional `days` preview1–3650 |
| POST `retention/configure/` (opt-in) | `version`, `changed`, saved `config`;409 stale/unapproved |

POST form fields: `event_id`, expected `attempt`, `reason`, `confirm=yes`; host
CSRF header required. Retention POST fields: `days`, `enabled=yes|no`, expected
`revision`, safe `reason`, `confirm=yes`. There is **no HTTP cleanup endpoint**.
The UI accepts `mountDashboard(root, {apiBase, theme, csrfToken, retention})`, or
the corresponding root data attributes (`data-retention="true"` opts in; Django
sets it). Hosts that do not opt in keep only Overview/Tasks. Serve allowlisted assets
via `dewey.dashboard.asset(name)`. `refresh()`/`destroy()` remain supported.

Theme follows host HTML data-theme/dark-light classes or system preference; Unfold
selects class mode, including chart colours. Serve `chart.js` alongside
`dashboard.js`; the included `chart.LICENSE.txt` carries third-party notices.
No CDN, npm install or consumer build. The UI styles only
its own root and does not replace host navigation.

- `TaskFilter`: hours1/24/168; limit1–100(default25); queue≤50; canonical lowercase
  status; ID/type-prefix search≤100. Validated opaque keyset `(created_at,id)` cursor;
  ledger order creation/ID descending, no offset/total count.
- Summary/queue counts: ready,scheduled,dispatching,processing,completed,dead.
  Queue adds `queue`, `configured`. Ready requires remaining budget, due schedule,
  unexpired deadline. Counts cover task **creation** in window.
- Owners: worker_id,active,expired_leases; active task claims only, capped20.
  Idle workers/health/total capacity remain unknown.
- Throughput: `recorded_from`, `bucket_seconds`, `buckets` of `at`, completed/dead
  nullable counts, coverage unavailable/partial/recorded. Missing history is not0.
- Task fields retain ID/type/queue/status/priority/attempts/budget, schedule/time,
  active owner/lease/deadline/latest_outcome. Detail adds error presence/safe summary,
  diagnostics,current diagnostic_event_id,retry allowed/explanation,events and
  events_truncated. Event has ID/kind/attempt/time, safe diagnostics and allowlisted
  retry audit fields; no arbitrary diagnostic/audit JSON is forwarded.
- Times ISO-8601 UTC, nullable when unavailable. Observations span multiple queries,
  not an atomic snapshot. HTTP failures fail closed; no cached private rows as fresh.
