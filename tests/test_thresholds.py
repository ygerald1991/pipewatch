"""Tests for pipewatch.thresholds and pipewatch.threshold_evaluator."""

import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import AlertSeverity
from pipewatch.thresholds import ThresholdConfig, load_thresholds, DEFAULT_THRESHOLDS
from pipewatch.threshold_evaluator import evaluate_metric_thresholds


def make_metric(pipeline_id="pipe-1", processed=100, failed=0, duration=10.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        processed=processed,
        failed=failed,
        duration_seconds=duration,
        timestamp=datetime.now(timezone.utc),
    )


class TestThresholdConfig:
    def test_defaults_are_sensible(self):
        cfg = DEFAULT_THRESHOLDS
        assert 0 < cfg.error_rate_warning < cfg.error_rate_critical
        assert cfg.throughput_critical < cfg.throughput_warning

    def test_for_pipeline_no_override_returns_same(self):
        cfg = ThresholdConfig()
        result = cfg.for_pipeline("unknown-pipe")
        assert result.error_rate_warning == cfg.error_rate_warning

    def test_for_pipeline_applies_override(self):
        cfg = ThresholdConfig(overrides={"pipe-x": {"error_rate_warning": 0.02}})
        result = cfg.for_pipeline("pipe-x")
        assert result.error_rate_warning == 0.02
        assert result.error_rate_critical == cfg.error_rate_critical

    def test_load_thresholds_from_dict(self):
        cfg = load_thresholds({"error_rate_warning": 0.03, "error_rate_critical": 0.20})
        assert cfg.error_rate_warning == 0.03
        assert cfg.error_rate_critical == 0.20

    def test_load_thresholds_empty_returns_defaults(self):
        cfg = load_thresholds({})
        assert cfg.error_rate_warning == DEFAULT_THRESHOLDS.error_rate_warning


class TestEvaluateMetricThresholds:
    def test_no_alerts_for_healthy_metric(self):
        metric = make_metric(processed=200, failed=2, duration=10.0)  # 1% error, 20/s
        alerts = evaluate_metric_thresholds(metric)
        assert alerts == []

    def test_warning_alert_on_moderate_error_rate(self):
        metric = make_metric(processed=100, failed=7, duration=10.0)  # 7%
        alerts = evaluate_metric_thresholds(metric)
        severities = [a.severity for a in alerts]
        assert AlertSeverity.WARNING in severities
        assert AlertSeverity.CRITICAL not in severities

    def test_critical_alert_on_high_error_rate(self):
        metric = make_metric(processed=100, failed=20, duration=10.0)  # 20%
        alerts = evaluate_metric_thresholds(metric)
        severities = [a.severity for a in alerts]
        assert AlertSeverity.CRITICAL in severities

    def test_critical_alert_on_low_throughput(self):
        metric = make_metric(processed=50, failed=0, duration=10.0)  # 5 rec/s
        alerts = evaluate_metric_thresholds(metric)
        severities = [a.severity for a in alerts]
        assert AlertSeverity.CRITICAL in severities

    def test_warning_alert_on_moderate_throughput(self):
        metric = make_metric(processed=250, failed=0, duration=10.0)  # 25 rec/s
        alerts = evaluate_metric_thresholds(metric)
        severities = [a.severity for a in alerts]
        assert AlertSeverity.WARNING in severities
        assert AlertSeverity.CRITICAL not in severities

    def test_pipeline_override_changes_threshold(self):
        cfg = ThresholdConfig(overrides={"pipe-1": {"error_rate_warning": 0.20}})
        metric = make_metric(pipeline_id="pipe-1", processed=100, failed=10)  # 10%
        alerts = evaluate_metric_thresholds(metric, config=cfg)
        # 10% is below the overridden 20% warning threshold
        assert all(a.severity != AlertSeverity.WARNING for a in alerts)
