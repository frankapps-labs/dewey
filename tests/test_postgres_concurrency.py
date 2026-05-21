"""Real-Postgres concurrency smoke tests."""


def test_skip_locked_claim_harness_prevents_double_claim(postgres_concurrency_harness):
    item_count = 40
    harness = postgres_concurrency_harness(item_count)

    result = harness.run_claimers(worker_count=4, batch_size=3)

    assert sorted(result.claimed_ids) == list(range(1, item_count + 1))
    assert len(result.claimed_ids) == item_count
    assert len(set(result.claimed_ids)) == item_count
    assert len(result.by_worker) == 4
