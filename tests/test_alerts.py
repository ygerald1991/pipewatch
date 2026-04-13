"""Unit tests for alert rule evaluation."""

import pytest
from pipewatch.alerts import (
    Alert,
    AlertRule,
    AlertSeverity,
    high_error_rate_rule,
    low_throughput_rule,
    pipeline_down_rule,
)
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(
    pipeline_id="pipe-1",
    total=100,
    failed=0,
    duration=10.0,
    status=PipelineStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
        status=status,
    )


class TestHighErrorRateRule:
    def test_no_alert_when_below_threshold(self):
        rule = high_error_rate_rule("pipe-1", threshold=0.1)
        metric = make_metric(total=100, failed=5)
        assert rule.evaluate(metric) is None

    def test_alert_when_above_threshold(self):
        rule = high_error_rate_rule("pipe-1", threshold=0.1)
        metric = make_metric(total=100, failed=15)
        alert = rule.evaluate(metric)
        assert alert is not None
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.rule_name == "high_error_rate"

    def test_alert_at_exact_threshold_not_triggered(self):
        rule = high_error_rate_rule("pipe-1", threshold=0.1)
        metric = make_metric(total=100, failed=10)
        assert rule.evaluate(metric) is None


class TestLowThroughputRule:
    def test_no_alert_when_above_minimum(self):
        rule = low_throughput_rule("pipe-1", min_records=50)
        metric = make_metric(total=1000, duration=10.0)
        assert rule.evaluate(metric) is None

    def test_alert_when_below_minimum(self):
        rule = low_throughput_rule("pipe-1", min_records=200)
        metric = make_metric(total=100, duration=10.0)
        alert = rule.evaluate(metric)
        assert alert is not None
        assert alert.severity == AlertSeverity.WARNING


class TestPipelineDownRule:
    def test_no_alert_when_ok(self):
        rule = pipeline_down_rule("pipe-1")
        metric = make_metric(status=PipelineStatus.OK)
        assert rule.evaluate(metric) is None

    def test_alert_when_failed(self):
        rule = pipeline_down_rule("pipe-1")
        metric = make_metric(status=PipelineStatus.FAILED)
        alert = rule.evaluate(metric)
        assert alert is not None
        assert alert.severity == AlertSeverity.CRITICAL
        assert "FAILED" in alert.message


def test_alert_str_format():
    rule = pipeline_down_rule("pipe-1")
    metric = make_metric(status=PipelineStatus.FAILED)
    alert = rule.evaluate(metric)
    assert "[CRITICAL]" in str(alert)
    assert "pipe-1" in str(alert)
