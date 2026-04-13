"""Tests for pipewatch.retention."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric
from pipewatch.retention import RetentionPolicy, apply_retention, prune_collector


def make_metric(pipeline_id: str, timestamp: datetime) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=timestamp,
        processed=100,
        failed=0,
        duration_seconds=1.0,
    )


NOW = datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture()
def collector() -> MetricsCollector:
    c = MetricsCollector(pipeline_id="pipe-a")
    c.record(make_metric("pipe-a", NOW - timedelta(hours=25)))  # stale
    c.record(make_metric("pipe-a", NOW - timedelta(hours=12)))  # fresh
    c.record(make_metric("pipe-a", NOW - timedelta(hours=1)))   # fresh
    return c


class TestRetentionPolicy:
    def test_default_max_age(self):
        policy = RetentionPolicy()
        assert policy.max_age_for("any") == timedelta(hours=24)

    def test_pipeline_override(self):
        policy = RetentionPolicy(pipeline_overrides={"pipe-a": 6.0})
        assert policy.max_age_for("pipe-a") == timedelta(hours=6)
        assert policy.max_age_for("pipe-b") == timedelta(hours=24)


class TestPruneCollector:
    def test_prunes_stale_metrics(self, collector):
        policy = RetentionPolicy(default_max_age_hours=24)
        pruned = prune_collector(collector, policy, now=NOW)
        assert pruned == 1
        assert len(collector._history) == 2

    def test_no_prune_when_all_fresh(self, collector):
        policy = RetentionPolicy(default_max_age_hours=48)
        pruned = prune_collector(collector, policy, now=NOW)
        assert pruned == 0
        assert len(collector._history) == 3

    def test_prune_all_when_window_zero(self, collector):
        policy = RetentionPolicy(default_max_age_hours=0)
        pruned = prune_collector(collector, policy, now=NOW)
        assert pruned == 3
        assert len(collector._history) == 0


class TestApplyRetention:
    def test_applies_to_all_collectors(self):
        policy = RetentionPolicy(default_max_age_hours=24)
        c1 = MetricsCollector(pipeline_id="p1")
        c2 = MetricsCollector(pipeline_id="p2")
        for c in (c1, c2):
            c.record(make_metric(c.pipeline_id, NOW - timedelta(hours=30)))
            c.record(make_metric(c.pipeline_id, NOW - timedelta(hours=1)))

        result = apply_retention({"p1": c1, "p2": c2}, policy, now=NOW)
        assert result == {"p1": 1, "p2": 1}

    def test_returns_empty_dict_for_no_collectors(self):
        policy = RetentionPolicy()
        result = apply_retention({}, policy, now=NOW)
        assert result == {}
