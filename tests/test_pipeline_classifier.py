import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_classifier import (
    classify_snapshot,
    classify_snapshots,
    ClassificationResult,
)
from pipewatch.classifier_reporter import render_classification_table, category_summary


def make_snapshot(pipeline_id: str, error_rate=None, throughput=None):
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status="healthy",
    )


class TestClassifySnapshot:
    def test_healthy_when_low_error_rate(self):
        s = make_snapshot("p1", error_rate=0.01, throughput=100.0)
        r = classify_snapshot(s)
        assert r.category == "healthy"

    def test_degraded_when_error_rate_at_10_percent(self):
        s = make_snapshot("p2", error_rate=0.10, throughput=50.0)
        r = classify_snapshot(s)
        assert r.category == "degraded"

    def test_critical_when_error_rate_at_20_percent(self):
        s = make_snapshot("p3", error_rate=0.20, throughput=10.0)
        r = classify_snapshot(s)
        assert r.category == "critical"

    def test_critical_above_20_percent(self):
        s = make_snapshot("p4", error_rate=0.55)
        r = classify_snapshot(s)
        assert r.category == "critical"

    def test_idle_when_zero_throughput_and_no_error(self):
        s = make_snapshot("p5", error_rate=None, throughput=0.0)
        r = classify_snapshot(s)
        assert r.category == "idle"

    def test_pipeline_id_preserved(self):
        s = make_snapshot("my-pipe", error_rate=0.0)
        r = classify_snapshot(s)
        assert r.pipeline_id == "my-pipe"

    def test_reason_is_non_empty(self):
        s = make_snapshot("p6", error_rate=0.05)
        r = classify_snapshot(s)
        assert r.reason

    def test_str_contains_category(self):
        s = make_snapshot("p7", error_rate=0.0)
        r = classify_snapshot(s)
        assert "healthy" in str(r)


class TestClassifySnapshots:
    def test_empty_returns_empty(self):
        assert classify_snapshots([]) == []

    def test_returns_one_result_per_snapshot(self):
        snaps = [make_snapshot(f"p{i}", error_rate=0.0) for i in range(5)]
        results = classify_snapshots(snaps)
        assert len(results) == 5

    def test_mixed_categories(self):
        snaps = [
            make_snapshot("a", error_rate=0.25),
            make_snapshot("b", error_rate=0.05),
            make_snapshot("c", error_rate=None, throughput=0.0),
        ]
        cats = {r.pipeline_id: r.category for r in classify_snapshots(snaps)}
        assert cats["a"] == "critical"
        assert cats["b"] == "healthy"
        assert cats["c"] == "idle"


class TestClassifierReporter:
    def test_empty_returns_no_data_message(self):
        out = render_classification_table([])
        assert "No classification" in out

    def test_output_contains_pipeline_id(self):
        s = make_snapshot("pipe-xyz", error_rate=0.0)
        out = render_classification_table([classify_snapshot(s)])
        assert "pipe-xyz" in out

    def test_category_summary_counts_correctly(self):
        snaps = [
            make_snapshot("a", error_rate=0.25),
            make_snapshot("b", error_rate=0.25),
            make_snapshot("c", error_rate=0.01),
        ]
        summary = category_summary(classify_snapshots(snaps))
        assert "critical: 2" in summary
        assert "healthy: 1" in summary

    def test_category_summary_empty(self):
        assert "No results" in category_summary([])
