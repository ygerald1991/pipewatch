"""Tests for pipewatch.drift_detector."""

import pytest
from pipewatch.drift_detector import DriftResult, detect_drift
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id="pipe-1", error_rate=0.0, throughput=100.0):
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=10,
        status=PipelineStatus.HEALTHY,
    )


class TestDetectDrift:
    def test_no_drift_when_identical(self):
        baseline = make_snapshot()
        current = make_snapshot()
        result = detect_drift(baseline, current)
        assert result.has_drift is False

    def test_drift_on_high_error_rate_increase(self):
        baseline = make_snapshot(error_rate=0.01)
        current = make_snapshot(error_rate=0.10)
        result = detect_drift(baseline, current, error_rate_threshold=0.05)
        assert result.has_drift is True
        assert result.error_rate_delta == pytest.approx(0.09)

    def test_no_drift_when_error_rate_change_within_threshold(self):
        baseline = make_snapshot(error_rate=0.01)
        current = make_snapshot(error_rate=0.03)
        result = detect_drift(baseline, current, error_rate_threshold=0.05)
        assert result.has_drift is False

    def test_drift_on_throughput_drop(self):
        baseline = make_snapshot(throughput=100.0)
        current = make_snapshot(throughput=70.0)
        result = detect_drift(baseline, current, throughput_threshold=0.20)
        assert result.has_drift is True
        assert result.throughput_delta == pytest.approx(-30.0)

    def test_no_drift_when_throughput_drop_within_threshold(self):
        baseline = make_snapshot(throughput=100.0)
        current = make_snapshot(throughput=85.0)
        result = detect_drift(baseline, current, throughput_threshold=0.20)
        assert result.has_drift is False

    def test_zero_baseline_throughput_does_not_raise(self):
        baseline = make_snapshot(throughput=0.0)
        current = make_snapshot(throughput=0.0)
        result = detect_drift(baseline, current)
        assert result.has_drift is False

    def test_str_no_drift(self):
        baseline = make_snapshot()
        current = make_snapshot()
        result = detect_drift(baseline, current)
        assert "No drift" in str(result)

    def test_str_with_drift_mentions_pipeline(self):
        baseline = make_snapshot(error_rate=0.0)
        current = make_snapshot(error_rate=0.20)
        result = detect_drift(baseline, current, error_rate_threshold=0.05)
        assert "pipe-1" in str(result)
        assert "Drift detected" in str(result)
