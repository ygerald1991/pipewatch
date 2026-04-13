"""Tests for pipewatch.latency_tracker."""

import pytest
from pipewatch.latency_tracker import LatencyStats, _percentile, compute_latency_stats
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(pipeline_id: str = "pipe-1", latency_ms: float | None = None) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        processed=100,
        failed=0,
        duration_seconds=60.0,
        status=PipelineStatus.HEALTHY,
        latency_ms=latency_ms,
    )


class TestPercentile:
    def test_empty_returns_zero(self):
        assert _percentile([], 95) == 0.0

    def test_single_value(self):
        assert _percentile([42.0], 95) == 42.0

    def test_p50_of_sorted_list(self):
        result = _percentile([10.0, 20.0, 30.0, 40.0, 50.0], 50)
        assert result == pytest.approx(30.0)

    def test_p95_of_uniform_list(self):
        values = [float(i) for i in range(1, 101)]
        result = _percentile(values, 95)
        assert result == pytest.approx(95.95, rel=1e-3)


class TestComputeLatencyStats:
    def test_returns_none_when_no_metrics(self):
        assert compute_latency_stats("pipe-1", []) is None

    def test_returns_none_when_no_latency_data(self):
        metrics = [make_metric(latency_ms=None), make_metric(latency_ms=None)]
        assert compute_latency_stats("pipe-1", metrics) is None

    def test_ignores_negative_latency(self):
        metrics = [make_metric(latency_ms=-10.0), make_metric(latency_ms=None)]
        assert compute_latency_stats("pipe-1", metrics) is None

    def test_correct_sample_count(self):
        metrics = [make_metric(latency_ms=100.0), make_metric(latency_ms=200.0)]
        result = compute_latency_stats("pipe-1", metrics)
        assert result is not None
        assert result.sample_count == 2

    def test_correct_mean(self):
        metrics = [make_metric(latency_ms=100.0), make_metric(latency_ms=300.0)]
        result = compute_latency_stats("pipe-1", metrics)
        assert result.mean_latency == pytest.approx(200.0)

    def test_correct_min_max(self):
        latencies = [50.0, 150.0, 300.0]
        metrics = [make_metric(latency_ms=v) for v in latencies]
        result = compute_latency_stats("pipe-1", metrics)
        assert result.min_latency == 50.0
        assert result.max_latency == 300.0

    def test_exceeds_threshold_when_p95_high(self):
        # 20 values: 19 at 100ms, 1 spike at 2000ms → p95 > 500
        metrics = [make_metric(latency_ms=100.0)] * 19 + [make_metric(latency_ms=2000.0)]
        result = compute_latency_stats("pipe-1", metrics, threshold_ms=500.0)
        assert result.exceeds_threshold is True

    def test_does_not_exceed_threshold_when_low(self):
        metrics = [make_metric(latency_ms=100.0) for _ in range(20)]
        result = compute_latency_stats("pipe-1", metrics, threshold_ms=500.0)
        assert result.exceeds_threshold is False

    def test_pipeline_id_preserved(self):
        metrics = [make_metric(pipeline_id="etl-prod", latency_ms=200.0)]
        result = compute_latency_stats("etl-prod", metrics)
        assert result.pipeline_id == "etl-prod"

    def test_str_contains_status_ok(self):
        metrics = [make_metric(latency_ms=100.0)]
        result = compute_latency_stats("pipe-1", metrics, threshold_ms=500.0)
        assert "OK" in str(result)
        assert "pipe-1" in str(result)

    def test_str_contains_status_slow(self):
        metrics = [make_metric(latency_ms=1000.0)]
        result = compute_latency_stats("pipe-1", metrics, threshold_ms=500.0)
        assert "SLOW" in str(result)
