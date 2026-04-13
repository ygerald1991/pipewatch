"""Tests for pipewatch.retention_session."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.retention import RetentionPolicy
from pipewatch.retention_session import RetentionSession


NOW = datetime(2024, 6, 1, 12, 0, 0)


def make_metric(pipeline_id: str, timestamp: datetime) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=timestamp,
        processed=50,
        failed=0,
        duration_seconds=0.5,
    )


@pytest.fixture()
def registry() -> PipelineRegistry:
    reg = PipelineRegistry()
    reg.record(make_metric("pipe-x", NOW - timedelta(hours=30)))
    reg.record(make_metric("pipe-x", NOW - timedelta(hours=2)))
    reg.record(make_metric("pipe-y", NOW - timedelta(hours=5)))
    return reg


class TestRetentionSession:
    def test_run_returns_pruned_counts(self, registry):
        policy = RetentionPolicy(default_max_age_hours=24)
        session = RetentionSession(registry, policy)
        result = session.run(now=NOW)
        assert result["pipe-x"] == 1
        assert result["pipe-y"] == 0

    def test_total_pruned_sums_counts(self, registry):
        policy = RetentionPolicy(default_max_age_hours=24)
        session = RetentionSession(registry, policy)
        result = session.run(now=NOW)
        assert session.total_pruned(result) == 1

    def test_last_run_updated_after_run(self, registry):
        session = RetentionSession(registry)
        assert session.last_run is None
        session.run(now=NOW)
        assert session.last_run == NOW

    def test_default_policy_applied_when_none_given(self, registry):
        session = RetentionSession(registry)
        assert session.policy.default_max_age_hours == 24.0

    def test_empty_registry_returns_empty_result(self):
        reg = PipelineRegistry()
        session = RetentionSession(reg)
        result = session.run(now=NOW)
        assert result == {}
