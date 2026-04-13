"""Tests for pipewatch.trend_analyzer."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.trend_analyzer import (
    TrendResult,
    analyze_error_rate_trend,
    analyze_throughput_trend,
    _linear_slope,
)


def make_metric(pipeline_id="pipe-1", processed=100, failed=0, duration=10.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
    )


class TestLinearSlope:
    def test_flat_series_returns_zero(self):
        assert _linear_slope([1.0, 1.0, 1.0]) == pytest.approx(0.0)

    def test_increasing_series_positive_slope(self):
        assert _linear_slope([0.0, 1.0, 2.0]) > 0

    def test_decreasing_series_negative_slope(self):
        assert _linear_slope([2.0, 1.0, 0.0]) < 0

    def test_single_value_returns_zero(self):
        assert _linear_slope([5.0]) == pytest.approx(0.0)


class TestAnalyzeErrorRateTrend:
    def test_returns_none_for_single_metric(self):
        m = make_metric(failed=5)
        assert analyze_error_rate_trend("p1", [m]) is None

    def test_stable_when_error_rate_unchanged(self):
        metrics = [make_metric(failed=5) for _ in range(5)]
        result = analyze_error_rate_trend("p1", metrics)
        assert result is not None
        assert result.direction == "stable"

    def test_degrading_when_error_rate_rises(self):
        metrics = [
            make_metric(processed=100, failed=i * 5)
            for i in range(1, 7)
        ]
        result = analyze_error_rate_trend("p1", metrics)
        assert result is not None
        assert result.direction == "degrading"

    def test_improving_when_error_rate_falls(self):
        metrics = [
            make_metric(processed=100, failed=(6 - i) * 5)
            for i in range(1, 7)
        ]
        result = analyze_error_rate_trend("p1", metrics)
        assert result is not None
        assert result.direction == "improving"

    def test_result_has_correct_pipeline_id(self):
        metrics = [make_metric(pipeline_id="my-pipe") for _ in range(3)]
        result = analyze_error_rate_trend("my-pipe", metrics)
        assert result.pipeline_id == "my-pipe"

    def test_result_sample_count_matches(self):
        metrics = [make_metric() for _ in range(8)]
        result = analyze_error_rate_trend("p1", metrics)
        assert result.sample_count == 8


class TestAnalyzeThroughputTrend:
    def test_returns_none_for_single_metric(self):
        m = make_metric()
        assert analyze_throughput_trend("p1", [m]) is None

    def test_improving_when_throughput_rises(self):
        metrics = [
            make_metric(processed=i * 20, duration=10.0)
            for i in range(1, 7)
        ]
        result = analyze_throughput_trend("p1", metrics)
        assert result is not None
        assert result.direction == "improving"

    def test_degrading_when_throughput_falls(self):
        metrics = [
            make_metric(processed=(7 - i) * 20, duration=10.0)
            for i in range(1, 7)
        ]
        result = analyze_throughput_trend("p1", metrics)
        assert result is not None
        assert result.direction == "degrading"

    def test_str_representation(self):
        r = TrendResult("p1", "error_rate", "degrading", 0.05, 5)
        assert "degrading" in str(r)
        assert "p1" in str(r)
