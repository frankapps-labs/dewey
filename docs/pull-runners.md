# Pull runners (candidate protocol 1)

The runner API lets a host gateway accept work from non-Python workers
without reimplementing Dewey's native state machine. It is a **server-side Python
transaction API**, not an HTTP server, direct-SQL protocol or first-party Go worker.
Existing gateways need an adapter and conformance testing; this is not a drop-in
replacement for their statements or an already-released capability.

## Local installed-wheel smoke

After installing the candidate wheel with SQLAlchemy/asyncpg in a fresh venv, run
its Python directly (not `uv run`/pytest, which may resolve the source checkout):

```sh
DEWEY_TEST_DATABASE_URL_ASYNC=postgresql+asyncpg://postgres:postgres@localhost:55440/dewey_test \
  /path/to/fresh-venv/bin/python tests/native/demo_runner.py
```

Use a disposable local Postgres database. The smoke creates/removes only its own
UUID schema and exercises the public async gateway lifecycle, host timeout
restoration, NULL retry scheduling and in-place ownership quarantine.

## Responsibilities

| Dewey | Host / runner |
|---|---|
| Due queue selection, assignment/execution generations | Authenticate/enroll the current worker process; choose allowed queues |
| Attempt budget, server policy/backoff, expiry, leases | Catalog/eligibility, resource reservation, worker pause/revoke |
| Fenced start/renew/complete/fail and lease recovery | Process containment/termination, output validation/storage, business receipts |
| Task transitions/history in the caller root | Commit task + domain/output/audit changes atomically, then acknowledge |

Use one execution/recovery owner per queue. Do not mix incompatible native and
custom SQL writers against the same obligations. A gateway's trusted maintenance
loop must run `recover`, and should run `tick` so [schedules](schedules.md) fire even
when no Python worker is running.

## Public imports

```python
from dewey.runner import (
    PROTOCOL_VERSION, RunnerAssignment, RunnerTiming, RunnerPolicy, FailureReport,
)
from dewey.sqlalchemy.runner import SQLAlchemyRunner, AsyncSQLAlchemyRunner
# Or: from dewey.django.runner import DjangoRunner
```

`PROTOCOL_VERSION == 1`. Assignment fields are `task_id` and `dispatch_token`.
Treat tokens as opaque secrets: no logging, public URLs or display. They do not
replace authentication. `RunnerTiming` shares the native worker's validated timing
contract (assignment/execution leases, renewal interval, DB wait, polling,
termination margin and hard task timeout); all fields are explicit and server-owned.
`task_timeout_seconds` is the server ceiling. Producers may set a positive,
finite `execution_timeout_s` on the task row (not in arbitrary payload fields).
At claim, the deadline is database now plus the smaller of that budget and the
ceiling; absent budgets retain the ceiling as before. Retries keep the row budget
and receive a fresh per-attempt deadline. Renewal maintains liveness but cannot
extend that deadline. Queue scope and timing remain server-owned.
Renewal/DB/polling/termination margins must fit inside leases. Drivers still need
bounded connection/network configuration; a SQL timeout cannot bound a lost COMMIT
acknowledgement or prove physical process termination.

`RunnerPolicy(owner, queues, timing, task_types=None)` is immutable, snapshots queues, rejects an
empty/bare-string queue scope, and accepts owner/name lengths matching the schema.
The host derives `owner` from the authenticated worker's **current instance**,
not a machine label or an unverified request body. Queue/timing overrides from
workers are not supported. `task_types` optionally narrows claims to the types the
runner's catalog can execute; other rows stay unclaimed for a runner that can. The library cannot enforce your enrollment policy.

## Caller-owned transactions

All methods return **provisional** results. They never commit, roll back, close or
own caller resources. A gateway must commit the root before delivering a ticket,
start acknowledgement, renewed lease or completion receipt. Do not execute a
workload before its start transaction commits. On **any error**, roll back the
whole root; never catch an error and commit provisional writes.

SQLAlchemy requires an already-open PostgreSQL Connection root, READ COMMITTED,
and a connection-bound `Session(join_transaction_mode="rollback_only")`. Engine-bound,
nested/savepoint, inactive and foreign participating mappings are rejected before
autoflush. After binding validation, pending host writes are flushed before fresh
row loads, so a pending host cancellation/queue change cannot be discarded by a
refresh. The host owns its Session's lifetime. Async has the identical contract
with AsyncConnection/AsyncSession. No application handler runs through `run_sync`.

```python
# policy is constructed from trusted enrollment/configuration, NOT request JSON.
with engine.connect() as connection:
    with connection.begin():
        with Session(bind=connection, join_transaction_mode="rollback_only") as session:
            runner = SQLAlchemyRunner(session, policy)
            tickets = runner.claim(1)
            # Host rechecks catalog/authorization/resources under this same root.
            # Reserve capacity here; if unavailable, raise and roll back the root.
    # Only now acknowledge/deliver tickets. No attempt is consumed by claim.
```

Django requires the chosen alias's existing **outermost** atomic block (no nested
blocks, including `savepoint=False`, rollback-pending state or autocommit):

```python
with transaction.atomic(using="jobs", durable=True):
    runner = DjangoRunner(policy, using="jobs")
    tickets = runner.claim(1)
    # Host domain/reservation/receipt writes must use the SAME alias/root.
# Deliver tickets only after successful outer commit.
```

The API bounds SQL waits using transaction-local timeouts and restores the caller's
previous statement/lock timeouts before returning, including Python errors. If a
SQL error aborts the root, PostgreSQL cannot restore settings until the caller
rolls it back; the original error is preserved. Keep the root short, with no
tool/network calls.
Fresh database time is sampled after row locks and after writes before returning;
`LeaseExpired` after provisional writes requires full caller rollback. The caller
must commit promptly and remains responsible for any later host writes, waits,
policy freshness and acknowledgement ordering outside the API call.

## Ownership quarantine

Readiness and recovery report up to 100 malformed native ownership rows per call,
ordered by task ID within the configured queues. Warnings contain the task ID and
an explicit invariant failure, never tokens or payloads. These rows remain in place
with ownership unchanged: they cannot be claimed, recovered or released. Healthy
recovery candidates are filtered separately before the requested limit, so even a
full diagnostic batch cannot starve them. Quarantine does not prove process stop
or repair a row; operators must investigate ownership before correcting it.
Schema/isolation failures still fail readiness. The diagnostic cap is not a full
queue audit; additional malformed rows may need a separate operator inspection.

## Operations

| Method | Provisional result / meaning |
|---|---|
| `check_readiness()` | Validate additive native schema and current isolation; no migration |
| `claim(limit=1)` | Assignment list; only due tasks in explicit queues; limits1–100; no attempt consumed |
| `start(ticket)` | Invocation dictionary or None; exactly one attempt admitted, opaque execution token |
| `renew(ticket, execution_token)` | `RunnerRenewal(cancel_requested: bool)` or None; full instance/assignment/execution fencing; processing only |
| `request_cancel(ticket, execution_token)` | bool; trusted host cooperative request on a live processing generation; no stop proof |
| `complete(ticket, execution_token)` | bool; same fences, live lease/deadline required |
| `fail(ticket, execution_token, FailureReport(...))` | bool; same fences, server policy classifies |
| `release(ticket)` | bool; unstarted DISPATCHING only, no attempt consumed; NEVER a process-stop acknowledgement |
| `recover(limit=100)` | Recovered IDs; **trusted host maintenance**, not a runner-request command |
| `tick(limit=20)` | Create task rows for due schedule ticks; one report per schedule (`schedule`, `created` ids, `missed`, `skipped`). Safe from any number of hosts; **trusted host maintenance** |

The invocation has `task_type`, JSON `args`/`kwargs`, `attempts`,
`execution_token`, `execution_timeout_s`, `execution_deadline_at` (aware datetime),
`execution_seconds_left` (the same deadline as a duration measured on the database
clock, so a runner can count it down on its own monotonic clock) and
`trace`: the creator's correlation fields, including a W3C `traceparent` (and
`tracestate`) when one was active at creation. Pass it to the runner so its spans and logs join the creator's trace. Alerts for what a call changed (a dead letter, a recovered execution, missed ticks)
reach the host's [alert hook](operations.md) only after the host commits the
transaction it lent the runner, and are dropped if it rolls back. `DjangoRunner`
sends them when the atomic block commits. SQLAlchemy has no hook that runs after a
successful commit of a caller-owned connection, so call `runner.deliver_alerts()`
once your commit has returned.

The host serializes/maps it to its existing transport and
approved workload catalog. Do not execute arbitrary client-supplied factory names,
commands or SQL. Helpers lock only rows in the configured scope even when the
caller supplies a known foreign ticket.

Assignment leases are fixed. There is no public assignment-renew shortcut:
`renew` requires a processing execution token. A remote caller cannot use the local
supervisor's token-less renewal or `abandon(stopped=True)` path. The local supervisor
keeps its existing trusted-parent behavior; both paths use the same native ledger.
A NULL `scheduled_for` means immediately due for PENDING and retryable FAILED
rows, matching the documented claiming predicate; claims never reset attempts.
Hard deadlines are set at claim and never extended by renew. Pre-start expiry and
exhaustion, interrupted/timed-out recovery and retry backoff retain native semantics.

`FailureReport(kind="transient" | "non_retryable", message="",
retry_after_seconds=None)` is data-only. Messages must be host-redacted and at most
2000 characters. A finite nonnegative transient retry-after (at most one year)
sets a floor, never an earlier retry than server policy. Arbitrary exception types,
client policy, new attempt budgets and client clocks are not accepted inputs.

## Atomic winner outputs and lost replies

In the **same root**, first obtain `complete(...) == True`, then write the validated
winner reference, execution-token prefix, command receipt and audit. Commit them
together. Any host validation/output/receipt failure must roll back completion.
Output validity and nontransactional object-store writes are host concerns; do not
publish a winner from task status alone. Keep stale attempts in separate prefixes.

A false/None result means that operation did not accept the supplied generation.
It does **not** establish whether a prior lost reply committed successfully. The
host must keep a stable idempotent command/winner receipt, compare replay intent,
and reconcile that receipt before retrying/acknowledging uncertain reports. Neither
an invalidated ticket nor a subprocess exit proves successful business output.

## Cancellation and common human/agent authority

The host-authorized `request_cancel` preserves ownership long enough for renewal
to deliver `cancel_requested=True`. It does not extend the lease itself. A requested
execution cannot win completion; a subsequent failure or recovery settles it as
DEAD/cancelled without retry. A trusted manual retry begins a new assignment with
the flag cleared. Existing `kill_task` still fences immediately; it is not a
cooperative notification. Host cancellation must separately represent requested,
no-new-admission, ownership invalidated, and actual worker-stop acknowledgement
(or unknown cleanup). DEAD is not proof of stopped
GPU work, released resources or reversed external effects. A running worker cannot
turn `release` into such evidence; losing ownership requires stop/containment.

Keep human CLI/API and agent MCP as thin transports to the **same host-authorized
command/read boundary**, with equal permissions, idempotency, receipts and audit.
MCP access grants no execution authority. This module is not that host service,
identity provider, MCP server, network authentication or operational rollout.
Qualify the installed candidate through the actual gateway and approved workload
before adopting it; library parity alone does not qualify a live executor cutover.
