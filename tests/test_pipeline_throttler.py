"""Tests for pipewatch.pipeline_throttler."""
import pytest
from pipewatch.pipeline_throttler import (
    ThrottlePolicy,
    ThrottleResult,
    PipelineThrottler,
)


# ---------------------------------------------------------------------------
# ThrottlePolicy
# ---------------------------------------------------------------------------

class TestThrottlePolicy:
    def test_default_max_events_is_sensible(self):
        policy = ThrottlePolicy()
        assert policy.default_max_events_per_second > 0

    def test_max_events_for_returns_default_when_no_override(self):
        policy = ThrottlePolicy(default_max_events_per_second=50.0)
        assert policy.max_events_for("pipe-a") == 50.0

    def test_max_events_for_returns_override_when_present(self):
        policy = ThrottlePolicy(
            default_max_events_per_second=50.0,
            overrides={"pipe-a": 10.0},
        )
        assert policy.max_events_for("pipe-a") == 10.0

    def test_override_does_not_affect_other_pipelines(self):
        policy = ThrottlePolicy(
            default_max_events_per_second=50.0,
            overrides={"pipe-a": 10.0},
        )
        assert policy.max_events_for("pipe-b") == 50.0


# ---------------------------------------------------------------------------
# ThrottleResult
# ---------------------------------------------------------------------------

class TestThrottleResult:
    def test_str_contains_pipeline_id(self):
        result = ThrottleResult("pipe-x", allowed=True, current_rate=20.0, limit=100.0)
        assert "pipe-x" in str(result)

    def test_str_shows_allowed_when_permitted(self):
        result = ThrottleResult("pipe-x", allowed=True, current_rate=20.0, limit=100.0)
        assert "allowed" in str(result)

    def test_str_shows_throttled_when_blocked(self):
        result = ThrottleResult("pipe-x", allowed=False, current_rate=200.0, limit=100.0)
        assert "throttled" in str(result)


# ---------------------------------------------------------------------------
# PipelineThrottler
# ---------------------------------------------------------------------------

class TestPipelineThrottler:
    def _make_throttler(self, limit: float = 100.0) -> PipelineThrottler:
        policy = ThrottlePolicy(default_max_events_per_second=limit)
        return PipelineThrottler(policy)

    def test_first_event_is_allowed(self):
        throttler = self._make_throttler(limit=10.0)
        result = throttler.check("pipe-a")
        assert result.allowed is True

    def test_result_contains_correct_pipeline_id(self):
        throttler = self._make_throttler()
        result = throttler.check("pipe-z")
        assert result.pipeline_id == "pipe-z"

    def test_result_limit_matches_policy(self):
        throttler = self._make_throttler(limit=42.0)
        result = throttler.check("pipe-a")
        assert result.limit == 42.0

    def test_burst_exhausts_tokens(self):
        """With a rate of 1/s the second immediate call should be throttled."""
        throttler = self._make_throttler(limit=1.0)
        first = throttler.check("pipe-a")
        second = throttler.check("pipe-a")
        assert first.allowed is True
        assert second.allowed is False

    def test_pipeline_ids_empty_initially(self):
        throttler = self._make_throttler()
        assert throttler.pipeline_ids() == []

    def test_pipeline_ids_populated_after_check(self):
        throttler = self._make_throttler()
        throttler.check("pipe-a")
        throttler.check("pipe-b")
        ids = throttler.pipeline_ids()
        assert "pipe-a" in ids
        assert "pipe-b" in ids

    def test_uses_default_policy_when_none_provided(self):
        throttler = PipelineThrottler()
        result = throttler.check("pipe-a")
        assert isinstance(result, ThrottleResult)

    def test_per_pipeline_override_respected(self):
        policy = ThrottlePolicy(
            default_max_events_per_second=100.0,
            overrides={"slow-pipe": 1.0},
        )
        throttler = PipelineThrottler(policy)
        throttler.check("slow-pipe")  # consumes the single token
        result = throttler.check("slow-pipe")
        assert result.allowed is False
        # fast pipe should still have tokens
        fast = throttler.check("fast-pipe")
        assert fast.allowed is True
