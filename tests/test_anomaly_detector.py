"""Tests for anomaly_detector and anomaly_session."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.anomaly_detector import detect_anomaly, analyze_metrics_for_anomalies
from pipewatch.anomaly_session import AnomalySession
from pipewatch.pipeline_registry import PipelineRegistry


def make_metric(pipeline_id="pipe-1", processed=100, failed=0, duration=10.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        processed=processed,
        failed=failed,
        duration_seconds=duration,
    )


class TestDetectAnomaly:
    def test_returns_none_when_insufficient_history(self):
        result = detect_anomaly("p1", "error_rate", [0.1, 0.2], 0.15)
        assert result is None

    def test_no_anomaly_for_normal_value(self):
        history = [0.05, 0.06, 0.05, 0.07, 0.06]
        result = detect_anomaly("p1", "error_rate", history, 0.06)
        assert result is not None
        assert result.is_anomaly is False

    def test_anomaly_detected_for_spike(self):
        history = [0.05, 0.06, 0.05, 0.06, 0.05]
        result = detect_anomaly("p1", "error_rate", history, 0.95)
        assert result is not None
        assert result.is_anomaly is True

    def test_z_score_is_zero_when_std_is_zero(self):
        history = [0.1, 0.1, 0.1, 0.1]
        result = detect_anomaly("p1", "metric", history, 0.1)
        assert result is not None
        assert result.z_score == 0.0
        assert result.is_anomaly is False

    def test_str_contains_pipeline_id(self):
        history = [0.05, 0.06, 0.05]
        result = detect_anomaly("pipe-42", "error_rate", history, 0.06)
        assert "pipe-42" in str(result)


class TestAnalyzeMetricsForAnomalies:
    def test_returns_empty_when_too_few_metrics(self):
        metrics = [make_metric() for _ in range(3)]
        results = analyze_metrics_for_anomalies("p1", metrics)
        assert results == []

    def test_returns_two_results_for_sufficient_history(self):
        metrics = [make_metric(processed=100, failed=5) for _ in range(5)]
        results = analyze_metrics_for_anomalies("p1", metrics)
        assert len(results) == 2

    def test_detects_error_rate_anomaly(self):
        normal = [make_metric(processed=100, failed=2) for _ in range(6)]
        spike = make_metric(processed=100, failed=95)
        results = analyze_metrics_for_anomalies("p1", normal + [spike])
        er_results = [r for r in results if r.metric_name == "error_rate"]
        assert er_results[0].is_anomaly is True


class TestAnomalySession:
    def _make_registry_with_history(self, pipeline_id, metrics):
        registry = PipelineRegistry()
        for m in metrics:
            registry.record(m)
        return registry

    def test_run_returns_results_per_pipeline(self):
        metrics = [make_metric(pipeline_id="p1") for _ in range(5)]
        registry = self._make_registry_with_history("p1", metrics)
        session = AnomalySession(registry=registry)
        results = session.run()
        assert "p1" in results

    def test_anomalous_pipelines_empty_when_healthy(self):
        metrics = [make_metric(pipeline_id="p1", failed=0) for _ in range(5)]
        registry = self._make_registry_with_history("p1", metrics)
        session = AnomalySession(registry=registry)
        session.run()
        assert session.anomalous_pipelines() == []

    def test_all_results_flattened(self):
        metrics = [make_metric(pipeline_id="p1") for _ in range(5)]
        registry = self._make_registry_with_history("p1", metrics)
        session = AnomalySession(registry=registry)
        session.run()
        assert isinstance(session.all_results(), list)
