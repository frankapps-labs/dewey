"""Read-only explanations of ledger facts, never worker-health or replay authority."""

from __future__ import annotations


def recovery_observation(task, now) -> dict[str, str]:
    """Explain a task observed at ``now`` without predicting worker availability.

    The caller supplies an ORM row (or equivalent attributes) and its observation
    clock. This does not mutate state, classify raw errors, or authorize commands.
    """

    def view(title, explanation, next_step, tone="neutral"):
        return {"title": title, "explanation": explanation, "next_step": next_step, "tone": tone}

    if task.status == "completed":
        return view(
            "Task completed",
            f"Attempt {task.attempts} completed in the task ledger.",
            "No further task attempt is scheduled. Check the business record for the actual outcome.",
            "good",
        )
    if task.status == "expired":
        return view(
            "Task expired",
            "The task's lifetime ended.",
            "Review the business obligation; no automatic retry remains.",
            "attention",
        )
    if task.status == "dead":
        return view(
            "Needs attention",
            "Automatic attempts have stopped for this task.",
            "Review the recorded failure and business outcome before considering a separately authorized retry.",
            "attention",
        )
    if task.status in ("processing", "dispatching"):
        if task.lease_expires_at is not None and task.lease_expires_at <= now:
            return view(
                "Awaiting recovery",
                "The recorded ownership lease has elapsed. This alone does not prove that the worker crashed.",
                "A worker must reconcile this attempt before eligible work can run again. Refresh to observe the result; worker availability is unknown.",
                "waiting",
            )
        return view(
            "Running" if task.status == "processing" else "Being assigned",
            f"Attempt {task.attempts} is recorded as {task.status}. This is a ledger observation, not a live worker-health check.",
            "Refresh to see the next recorded outcome. Do not start a duplicate task while this attempt is unresolved.",
        )
    if task.status not in ("pending", "failed"):
        return view(
            "State unavailable",
            "This task state is not recognized by this view.",
            "Inspect the host's supported task contract.",
            "attention",
        )
    if any(
        value is not None
        for value in (
            task.execution_token,
            task.dispatch_token,
            task.worker_id,
            task.lease_expires_at,
            task.execution_deadline_at,
        )
    ):
        return view(
            "Ownership unresolved",
            "An ownership marker remains on this task.",
            "Wait for reconciliation; this view cannot establish retry eligibility.",
            "waiting",
        )
    if task.expires_at is not None and task.expires_at <= now:
        return view(
            "Lifetime elapsed",
            "The task's expiry has passed; the ledger has not yet recorded an expired state.",
            "No new attempt is eligible. Review the business obligation.",
            "attention",
        )
    if task.attempts >= task.max_attempts:
        return view(
            "Attempt budget exhausted",
            "No automatic attempts remain in the recorded budget.",
            "Review the recorded failure and business outcome. Refresh for any terminal-state reconciliation.",
            "attention",
        )
    if task.scheduled_for is not None and task.scheduled_for > now:
        return view(
            "Waiting to retry" if task.status == "failed" else "Scheduled",
            "A future execution time is recorded; this is not evidence of a human-approval wait.",
            "Eligible no earlier than the scheduled time, while unexpired and within budget. A serving worker is still required.",
            "waiting",
        )
    return view(
        "Ready to retry" if task.status == "failed" else "Ready",
        "The recorded schedule, lifetime and attempt budget permit another attempt.",
        "A worker serving this queue can claim it. Worker availability and recovery time are not known from task rows.",
        "waiting",
    )
