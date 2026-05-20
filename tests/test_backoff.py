"""Tests for backoff calculation."""

from datetime import timedelta

from dewey.core.backoff import retry_delay


class TestBackoff:
    def test_exponential_sequence(self):
        # jitter=0 for deterministic tests
        assert retry_delay(0, jitter=0) == timedelta(seconds=120)  # 2min
        assert retry_delay(1, jitter=0) == timedelta(seconds=240)  # 4min
        assert retry_delay(2, jitter=0) == timedelta(seconds=480)  # 8min
        assert retry_delay(3, jitter=0) == timedelta(seconds=960)  # 16min
        assert retry_delay(4, jitter=0) == timedelta(seconds=1920)  # 32min

    def test_capped_at_max_delay(self):
        # Default max is 3600s (1h)
        result = retry_delay(10, jitter=0)  # 120 * 1024 = 122880 → capped to 3600
        assert result == timedelta(seconds=3600)

    def test_custom_base_delay(self):
        assert retry_delay(0, base_delay=60, jitter=0) == timedelta(seconds=60)
        assert retry_delay(1, base_delay=60, jitter=0) == timedelta(seconds=120)

    def test_custom_max_delay(self):
        assert retry_delay(10, max_delay=600, jitter=0) == timedelta(seconds=600)

    def test_zero_attempts(self):
        # First retry should be base_delay
        result = retry_delay(0, base_delay=30, jitter=0)
        assert result == timedelta(seconds=30)


class TestBackoffJitter:
    def test_jitter_adds_variance(self):
        """With jitter > 0, repeated calls produce different values."""
        results = set()
        for _ in range(20):
            delay = retry_delay(2, jitter=0.25)
            results.add(delay.total_seconds())
        # With 25% jitter on 480s, range is 360-600. Should get multiple distinct values.
        assert len(results) > 1

    def test_jitter_stays_within_bounds(self):
        """Jitter should stay within ± jitter_fraction of the base delay."""
        base = 120  # attempts=0
        jitter_frac = 0.25
        for _ in range(100):
            delay = retry_delay(0, base_delay=base, jitter=jitter_frac)
            seconds = delay.total_seconds()
            assert seconds >= base * (1 - jitter_frac)
            assert seconds <= base * (1 + jitter_frac)

    def test_jitter_capped_at_max_delay(self):
        """Even with jitter, delay should never exceed max_delay."""
        for _ in range(200):
            delay = retry_delay(10, max_delay=3600, jitter=0.5)
            assert delay.total_seconds() <= 3600

    def test_jitter_never_negative(self):
        """Even with high jitter, delay should never be negative."""
        for _ in range(100):
            delay = retry_delay(0, base_delay=10, jitter=1.0)
            assert delay.total_seconds() >= 0

    def test_zero_jitter_is_deterministic(self):
        """jitter=0 produces identical results every time."""
        results = [retry_delay(2, jitter=0) for _ in range(10)]
        assert len({r.total_seconds() for r in results}) == 1
