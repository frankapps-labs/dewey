# Notifications

> **Experimental.** This layer is not covered by the stability expectations that apply to
> the rest of Dewey, and it may change shape or be replaced in a later release. It is a
> second ledger with its own state machine and its own sweep, and it predates the
> dispatcher: notifications are **not** dispatcher-driven and have no `DISPATCHING` state.
>
> The intended direction is now settled: a notification should simply *be* a task — a
> task type whose handler sends through a channel, using `TaskPolicy` for retries instead
> of a parallel retry engine — with per-attempt history promoted to the task ledger for
> every task type rather than living only here. Expect this layer to be replaced along
> those lines in a later release, so build anything load-bearing on the task API.

Dewey includes a notification layer that tracks delivery of messages through channels (email, webhook, Slack, etc.) with per-attempt tracking, retry, and dead-lettering.

### Channel Protocol

Implement the `Channel` protocol to add a delivery channel:

```python
from dewey.core.notifications import Channel, ChannelResult

class EmailChannel:
    @property
    def name(self) -> str:
        return "email"

    def send(self, recipient, subject, body, payload) -> ChannelResult:
        # Your delivery logic here
        send_email(to=recipient, subject=subject, body=body)
        return ChannelResult(success=True, response_data={"message_id": "msg-123"})
```

### Event Registry

Map event types to channels with recipient resolution and body rendering:

```python
from dewey.core.notifications import ChannelRegistry

registry = ChannelRegistry()
registry.register_channel(email_channel)
registry.register_channel(slack_channel)

registry.on(
    "order.confirmed",
    channel="email",
    recipient=lambda p: p["customer_email"],
    render=lambda evt, p: ("Order confirmed", f"Order {p['order_id']} is confirmed."),
)
registry.on(
    "task.dead",
    channel="slack",
    recipient=lambda p: "#alerts",
    render=lambda evt, p: ("", f"Task {p.get('task_id', '?')} is dead-lettered."),
)
```

### Creating & Sending Notifications — SQLAlchemy

```python
from dewey.sqlalchemy.notifications import (
    create_notifications_for_event,
    send_notification,
    process_notification,
    sweep_notifications,
)

# Create notifications for all registered channels
with Session(engine) as session:
    notifications = create_notifications_for_event(
        session,
        registry=registry,
        event_type="order.confirmed",
        payload={"customer_email": "buyer@example.com", "order_id": "ORD-123"},
        task_id=task.id,  # optional: link to a task
    )
    session.commit()

    # Send each notification
    for notif in notifications:
        channel = registry.get_channel(notif.channel)
        send_notification(session, notif.id, channel)

# Or use process_notification to auto-resolve the channel:
process_notification(session, notification_id, registry)

# Sweep picks up failed/stuck notifications (run periodically)
result = sweep_notifications(session)
```

### Creating & Sending Notifications — Django

```python
from dewey.django.notifications import (
    create_notifications_for_event,
    send_notification,
    process_notification,
    sweep_notifications,
)

# Same API, no session parameter:
notifications = create_notifications_for_event(
    registry=registry,
    event_type="order.confirmed",
    payload={"customer_email": "buyer@example.com", "order_id": "ORD-123"},
)

for notif in notifications:
    channel = registry.get_channel(notif.channel)
    send_notification(notif.id, channel)
```

### Scheduling a notification for later

Like tasks, notifications accept a `scheduled_for` datetime. The sender and
the sweep both refuse to deliver a notification whose `scheduled_for` is in
the future, leaving it `PENDING` until the time arrives:

```python
from datetime import datetime, timedelta, UTC
from dewey.sqlalchemy.notifications import create_notification

with Session(engine) as session:
    notif = create_notification(
        session,
        event_type="order.reminder",
        channel="email",
        recipient="buyer@example.com",
        scheduled_for=datetime.now(UTC) + timedelta(days=1),
    )
    session.commit()
```

The same parameter is available on the async and Django variants.

### Per-Attempt Tracking

Every delivery attempt is logged with status, error, and response data:

```python
from dewey.sqlalchemy.notifications import get_notification_attempts

attempts = get_notification_attempts(session, notification_id)
for attempt in attempts:
    print(f"Attempt {attempt.attempt_number}: {attempt.status} — {attempt.error}")
```

### Notification Queries

| Function | Description |
|----------|-------------|
| `get_notification_stats(...)` | Counts by status |
| `get_notification(..., id)` | Single notification detail |
| `get_notification_attempts(..., id)` | All delivery attempts |
| `get_notifications_for_task(..., task_id)` | Notifications linked to a task |
| `get_pending_notifications(...)` | Ready to send |
| `get_failed_notifications(...)` | Failed, eligible for retry |
| `get_dead_notifications(...)` | Dead-lettered |
| `retry_notification(..., id)` | Reset failed/dead → pending |
| `kill_notification(..., id)` | Force to dead |
| `purge_sent_notifications(...)` | Delete old sent notifications |
