"""Retry delay calculation — exponential backoff with jitter and cap."""

import random
from collections.abc import Callable
from datetime import timedelta

# Defaults
DEFAULT_BASE_DELAY_SECONDS = 120  # 2 minutes
DEFAULT_MAX_DELAY_SECONDS = 3600  # 1 hour cap
DEFAULT_JITTER_FRACTION = 0.25  # ±25% jitter

# Notifications retry faster than tasks because they're user-facing.
DEFAULT_NOTIFICATION_BASE_DELAY_SECONDS = 60  # 1 minute
DEFAULT_NOTIFICATION_MAX_DELAY_SECONDS = 1800  # 30 minute cap


# Signature for a custom backoff strategy.
# Given the number of attempts so far, return how long to wait before retrying.
BackoffFn = Callable[[int], timedelta]


def default_task_backoff(attempts: int) -> timedelta:
    """Default backoff used by process_task / process_task_async when no
    `backoff` argument is supplied. 2 min base, 1 hr cap, ±25% jitter."""
    return retry_delay(attempts)


def default_notification_backoff(attempts: int) -> timedelta:
    """Default backoff used by send_notification / send_notification_async
    when no `backoff` argument is supplied. 1 min base, 30 min cap, ±25% jitter."""
    return retry_delay(
        attempts,
        base_delay=DEFAULT_NOTIFICATION_BASE_DELAY_SECONDS,
        max_delay=DEFAULT_NOTIFICATION_MAX_DELAY_SECONDS,
    )


def retry_delay(
    attempts: int,
    base_delay: int = DEFAULT_BASE_DELAY_SECONDS,
    max_delay: int = DEFAULT_MAX_DELAY_SECONDS,
    jitter: float = DEFAULT_JITTER_FRACTION,
) -> timedelta:
    """
    Exponential backoff with jitter: base * 2^attempts ± jitter%, capped at max_delay.

    Jitter prevents thundering herd when many tasks fail simultaneously.
    Set jitter=0 for deterministic behavior (e.g. in tests).

    attempts=0 → ~2min, attempts=1 → ~4min, attempts=2 → ~8min, ...
    """
    delay = min(base_delay * (2**attempts), max_delay)
    if jitter > 0:
        jitter_amount = delay * jitter
        delay = delay + random.uniform(-jitter_amount, jitter_amount)
        delay = max(0, min(delay, max_delay))  # Never negative, never above cap
    return timedelta(seconds=delay)
