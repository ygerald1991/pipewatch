"""Tests for pipewatch.rate_limiter."""

from datetime import datetime, timedelta

import pytest

from pipewatch.rate_limiter import RateLimitPolicy, RateLimiter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_limiter(max_alerts: int = 3, window_seconds: int = 60) -> RateLimiter:
    policy = RateLimitPolicy(max_alerts=max_alerts, window_seconds=window_seconds)
    return RateLimiter(policy=policy)


T0 = datetime(2024, 1, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# RateLimitPolicy
# ---------------------------------------------------------------------------

class TestRateLimitPolicy:
    def test_defaults_are_sensible(self):
        policy = RateLimitPolicy()
        assert policy.max_alerts > 0
        assert policy.window_seconds > 0

    def test_limits_for_returns_defaults_when_no_override(self):
        policy = RateLimitPolicy(max_alerts=5, window_seconds=300)
        assert policy.limits_for("pipe-a") == (5, 300)

    def test_limits_for_returns_override_when_present(self):
        policy = RateLimitPolicy(
            max_alerts=5,
            window_seconds=300,
            pipeline_overrides={"pipe-a": (1, 60)},
        )
        assert policy.limits_for("pipe-a") == (1, 60)
        assert policy.limits_for("pipe-b") == (5, 300)


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def test_first_alert_is_always_allowed(self):
        limiter = make_limiter(max_alerts=1)
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is True

    def test_alert_blocked_after_max_reached(self):
        limiter = make_limiter(max_alerts=2)
        limiter.record("pipe-a", "warning", now=T0)
        limiter.record("pipe-a", "warning", now=T0)
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is False

    def test_alert_allowed_after_window_expires(self):
        limiter = make_limiter(max_alerts=1, window_seconds=60)
        limiter.record("pipe-a", "warning", now=T0)
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is False

        future = T0 + timedelta(seconds=61)
        assert limiter.is_allowed("pipe-a", "warning", now=future) is True

    def test_different_severities_tracked_independently(self):
        limiter = make_limiter(max_alerts=1)
        limiter.record("pipe-a", "critical", now=T0)
        assert limiter.is_allowed("pipe-a", "critical", now=T0) is False
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is True

    def test_different_pipelines_tracked_independently(self):
        limiter = make_limiter(max_alerts=1)
        limiter.record("pipe-a", "warning", now=T0)
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is False
        assert limiter.is_allowed("pipe-b", "warning", now=T0) is True

    def test_current_count_starts_at_zero(self):
        limiter = make_limiter()
        assert limiter.current_count("pipe-x", "warning") == 0

    def test_current_count_increments_on_record(self):
        limiter = make_limiter()
        limiter.record("pipe-a", "warning", now=T0)
        limiter.record("pipe-a", "warning", now=T0)
        assert limiter.current_count("pipe-a", "warning") == 2

    def test_reset_clears_bucket(self):
        limiter = make_limiter(max_alerts=1)
        limiter.record("pipe-a", "warning", now=T0)
        limiter.reset("pipe-a", "warning")
        assert limiter.current_count("pipe-a", "warning") == 0
        assert limiter.is_allowed("pipe-a", "warning", now=T0) is True

    def test_window_resets_count(self):
        limiter = make_limiter(max_alerts=2, window_seconds=30)
        limiter.record("pipe-a", "critical", now=T0)
        limiter.record("pipe-a", "critical", now=T0)
        future = T0 + timedelta(seconds=31)
        limiter.record("pipe-a", "critical", now=future)
        assert limiter.current_count("pipe-a", "critical") == 1
