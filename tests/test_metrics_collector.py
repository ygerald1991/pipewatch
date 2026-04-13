"""Tests for PipelineMetric and MetricsCollector."""

from datetime import datetime, timedelta

import pytest

from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(pipeline_id="pipe-1", rows_processed=1000, rows_failed=0,
                duration_seconds=60.0, lag_seconds=10.0, error_message=None) -> PipelineMetric:
    m = PipelineMetric(
        pipeline_id=pipeline_id,
        rows_processed=rows_processed,
        rows_failed=rows_failed,
        duration_seconds=duration_seconds,
        lag_seconds=lag_seconds,
        error_message=error_message,
    )
    m.evaluate_status()
    return m


class TestPipelineMetric:
    def test_error_rate_zero_when_no_failures(self):
        m = make_metric(rows_processed=500, rows_failed=0)
        assert m.error_rate == 0.0

    def test_error_rate_calculation(self):
        m = make_metric(rows_processed=900, rows_failed=100)
        assert m.error_rate == pytest.approx(0.1)

    def test_throughput_calculation(self):
        m = make_metric(rows_processed=300, duration_seconds=60.0)
        assert m.throughput == pytest.approx(5.0)

    def test_throughput_zero_when_no_duration(self):
        m = make_metric(rows_processed=100, duration_seconds=0.0)
        assert m.throughput == 0.0

    def test_evaluate_status_healthy(self):
        m = make_metric(rows_processed=1000, rows_failed=0, lag_seconds=5.0)
        assert m.evaluate_status() == PipelineStatus.HEALTHY

    def test_evaluate_status_degraded_high_lag(self):
        m = make_metric(rows_processed=1000, lag_seconds=400.0)
        assert m.evaluate_status() == PipelineStatus.DEGRADED

    def test_evaluate_status_failing_on_error_message(self):
        m = make_metric(error_message="Connection refused")
        assert m.evaluate_status() == PipelineStatus.FAILING

    def test_to_dict_contains_required_keys(self):
        m = make_metric()
        d = m.to_dict()
        for key in ("pipeline_id", "timestamp", "rows_processed", "rows_failed",
                    "error_rate", "throughput", "status", "lag_seconds"):
            assert key in d


class TestMetricsCollector:
    def test_record_and_latest(self):
        collector = MetricsCollector()
        m = make_metric(pipeline_id="pipe-a")
        collector.record(m)
        assert collector.latest("pipe-a") is m

    def test_latest_returns_none_for_unknown_pipeline(self):
        collector = MetricsCollector()
        assert collector.latest("nonexistent") is None

    def test_history_respects_limit(self):
        collector = MetricsCollector()
        for _ in range(10):
            collector.record(make_metric(pipeline_id="pipe-b"))
        assert len(collector.history("pipe-b", limit=5)) == 5

    def test_unhealthy_pipelines_filters_correctly(self):
        collector = MetricsCollector()
        collector.record(make_metric(pipeline_id="ok", rows_processed=100))
        collector.record(make_metric(pipeline_id="bad", error_message="timeout"))
        unhealthy_ids = [m.pipeline_id for m in collector.unhealthy_pipelines()]
        assert "bad" in unhealthy_ids
        assert "ok" not in unhealthy_ids

    def test_summary_returns_all_pipelines(self):
        collector = MetricsCollector()
        collector.record(make_metric(pipeline_id="p1"))
        collector.record(make_metric(pipeline_id="p2"))
        ids = [s["pipeline_id"] for s in collector.summary()]
        assert set(ids) == {"p1", "p2"}
