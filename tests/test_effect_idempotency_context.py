"""Real ledger retries retain identity across Django and both SQLAlchemy paths."""

import pytest

import dewey


def effect_handler():
    """Simulate a provider that deduplicates keys after a lost first response."""
    contexts = []
    receipts = {}

    def handler():
        context = dewey.current_task()
        contexts.append(context)
        receipts.setdefault(context.effect_key("capture"), "synthetic-receipt")
        if context.attempt == 1:
            raise ValueError("Synthetic provider response lost after effect")

    return handler, contexts, receipts


def assert_idempotent_retry(task_id, contexts, receipts):
    assert [context.task_id for context in contexts] == [task_id, task_id]
    assert [context.attempt for context in contexts] == [1, 2]
    assert len(receipts) == 1
    assert contexts[0].effect_key("capture") == contexts[1].effect_key("capture")
    with pytest.raises(RuntimeError):
        dewey.current_task()
