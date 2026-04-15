"""Tests for pipeline_aggregator and aggregator_reporter."""
from __future__ import annotations

import datetime
from typing import List

import pytest

from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_aggregator import aggregate_snapshots, AggregateStats
from pipewatch.aggregator_reporter import render_aggregate_report, overall_aggregate_status


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    throughput: float = 100.0,
    metric_count: int = 10,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.datetime(2024, 1, 1, 12, 0, 0),
        error_rate=error_rate,
        throughput=throughput,
        metric_count=metric_count,
    )


class TestAggregateSnapshots:
    def test_empty_list_returns_zero_counts(self):
        result = aggregate_snapshots([])
        assert result.pipeline_count == 0
        assert result.total_metrics == 0
        assert result.avg_error_rate is None

    def test_single_snapshot_statistics(self):
        snap = make_snapshot("pipe-1", error_rate=0.1, throughput=200.0, metric_count=5)
        result = aggregate_snapshots([snap])
        assert result.pipeline_count == 1
        assert result.total_metrics == 5
        assert result.avg_error_rate == pytest.approx(0.1)
        assert result.max_error_rate == pytest.approx(0.1)
        assert result.min_error_rate == pytest.approx(0.1)
        assert result.avg_throughput == pytest.approx(200.0)

    def test_avg_error_rate_across_multiple(self):
        snaps = [
            make_snapshot("p1", error_rate=0.0),
            make_snapshot("p2", error_rate=0.2),
        ]
        result = aggregate_snapshots(snaps)
        assert result.avg_error_rate == pytest.approx(0.1)

    def test_max_and_min_error_rate(self):
        snaps = [
            make_snapshot("p1", error_rate=0.01),
            make_snapshot("p2", error_rate=0.5),
            make_snapshot("p3", error_rate=0.1),
        ]
        result = aggregate_snapshots(snaps)
        assert result.max_error_rate == pytest.approx(0.5)
        assert result.min_error_rate == pytest.approx(0.01)

    def test_degraded_count_above_threshold(self):
        snaps = [
            make_snapshot("p1", error_rate=0.01),
            make_snapshot("p2", error_rate=0.06),
            make_snapshot("p3", error_rate=0.10),
        ]
        result = aggregate_snapshots(snaps)
        assert result.degraded_count == 2

    def test_pipeline_ids_present(self):
        snaps = [make_snapshot("alpha"), make_snapshot("beta")]
        result = aggregate_snapshots(snaps)
        assert set(result.pipeline_ids) == {"alpha", "beta"}

    def test_total_metrics_summed(self):
        snaps = [
            make_snapshot("p1", metric_count=3),
            make_snapshot("p2", metric_count=7),
        ]
        result = aggregate_snapshots(snaps)
        assert result.total_metrics == 10


class TestAggregatorReporter:
    def test_empty_stats_returns_no_data_message(self):
        stats = aggregate_snapshots([])
        output = render_aggregate_report(stats)
        assert "No pipeline data" in output

    def test_output_contains_pipeline_count(self):
        snaps = [make_snapshot("p1"), make_snapshot("p2")]
        stats = aggregate_snapshots(snaps)
        output = render_aggregate_report(stats)
        assert "2" in output

    def test_overall_status_healthy_when_no_degraded(self):
        snaps = [make_snapshot("p1", error_rate=0.0)]
        stats = aggregate_snapshots(snaps)
        assert overall_aggregate_status(stats) == "HEALTHY"

    def test_overall_status_critical_when_majority_degraded(self):
        snaps = [
            make_snapshot("p1", error_rate=0.5),
            make_snapshot("p2", error_rate=0.5),
            make_snapshot("p3", error_rate=0.5),
        ]
        stats = aggregate_snapshots(snaps)
        assert overall_aggregate_status(stats) == "CRITICAL"

    def test_overall_status_warning_when_few_degraded(self):
        snaps = [
            make_snapshot("p1", error_rate=0.5),
            make_snapshot("p2", error_rate=0.0),
            make_snapshot("p3", error_rate=0.0),
            make_snapshot("p4", error_rate=0.0),
        ]
        stats = aggregate_snapshots(snaps)
        assert overall_aggregate_status(stats) == "WARNING"

    def test_overall_status_unknown_when_empty(self):
        stats = aggregate_snapshots([])
        assert overall_aggregate_status(stats) == "UNKNOWN"
