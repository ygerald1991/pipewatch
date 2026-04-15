"""Tests for pipeline_comparator and comparator_reporter."""

from __future__ import annotations

import pytest

from pipewatch.pipeline_comparator import (
    ComparisonResult,
    compare_snapshots,
    outlier_pipelines,
)
from pipewatch.comparator_reporter import render_comparison_table, outlier_summary
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, error_rate: float, throughput: float) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status="healthy",
        captured_at=0.0,
    )


class TestCompareSnapshots:
    def test_empty_returns_empty(self):
        assert compare_snapshots([]) == []

    def test_single_snapshot_no_outlier(self):
        snap = make_snapshot("pipe-a", error_rate=0.1, throughput=100.0)
        results = compare_snapshots([snap])
        assert all(not r.is_outlier for r in results)

    def test_results_contain_both_metrics(self):
        snaps = [
            make_snapshot("pipe-a", 0.1, 100.0),
            make_snapshot("pipe-b", 0.2, 200.0),
        ]
        results = compare_snapshots(snaps)
        metrics = {r.metric for r in results}
        assert "error_rate" in metrics
        assert "throughput" in metrics

    def test_outlier_detected_for_spike(self):
        snaps = [
            make_snapshot("pipe-a", 0.01, 100.0),
            make_snapshot("pipe-b", 0.01, 100.0),
            make_snapshot("pipe-c", 0.01, 100.0),
            make_snapshot("pipe-spike", 0.99, 100.0),  # obvious outlier
        ]
        results = compare_snapshots(snaps, outlier_std_threshold=1.5)
        outlier_ids = outlier_pipelines(results)
        assert "pipe-spike" in outlier_ids

    def test_no_outlier_when_values_uniform(self):
        snaps = [
            make_snapshot("pipe-a", 0.05, 100.0),
            make_snapshot("pipe-b", 0.05, 100.0),
            make_snapshot("pipe-c", 0.05, 100.0),
        ]
        results = compare_snapshots(snaps)
        assert outlier_pipelines(results) == []

    def test_delta_sign_is_correct(self):
        snaps = [
            make_snapshot("pipe-low", 0.0, 100.0),
            make_snapshot("pipe-high", 1.0, 100.0),
        ]
        results = compare_snapshots(snaps)
        er_results = {r.pipeline_id: r for r in results if r.metric == "error_rate"}
        assert er_results["pipe-high"].delta > 0
        assert er_results["pipe-low"].delta < 0

    def test_outlier_pipelines_unique(self):
        snaps = [
            make_snapshot("pipe-a", 0.01, 1.0),
            make_snapshot("pipe-b", 0.01, 1.0),
            make_snapshot("pipe-spike", 0.99, 9999.0),
        ]
        results = compare_snapshots(snaps, outlier_std_threshold=1.0)
        ids = outlier_pipelines(results)
        assert ids.count("pipe-spike") == 1


class TestComparatorReporter:
    def test_empty_results_returns_no_data_message(self):
        output = render_comparison_table([])
        assert "No comparison data" in output

    def test_table_contains_pipeline_id(self):
        snaps = [
            make_snapshot("alpha", 0.1, 50.0),
            make_snapshot("beta", 0.2, 60.0),
        ]
        results = compare_snapshots(snaps)
        output = render_comparison_table(results)
        assert "alpha" in output
        assert "beta" in output

    def test_outlier_summary_no_outliers(self):
        snaps = [make_snapshot("p", 0.05, 100.0)]
        results = compare_snapshots(snaps)
        summary = outlier_summary(results)
        assert "within normal range" in summary

    def test_outlier_summary_lists_outlier(self):
        snaps = [
            make_snapshot("pipe-a", 0.01, 100.0),
            make_snapshot("pipe-b", 0.01, 100.0),
            make_snapshot("pipe-spike", 0.99, 100.0),
        ]
        results = compare_snapshots(snaps, outlier_std_threshold=1.0)
        summary = outlier_summary(results)
        assert "pipe-spike" in summary
