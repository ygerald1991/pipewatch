"""Tests for pipewatch.reporter module."""

from datetime import datetime
from unittest.mock import patch

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.reporter import generate_report, PipelineReport


def make_metric(
    records_processed: int = 100,
    records_failed: int = 0,
    duration_seconds: float = 10.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id="test-pipe",
        timestamp=datetime.utcnow(),
        records_processed=records_processed,
        records_failed=records_failed,
        duration_seconds=duration_seconds,
    )


class TestGenerateReport:
    def test_report_has_correct_pipeline_id(self):
        metric = make_metric()
        report = generate_report("my-pipeline", metric)
        assert report.pipeline_id == "my-pipeline"

    def test_report_healthy_status(self):
        metric = make_metric(records_processed=100, records_failed=1)
        report = generate_report("pipe", metric)
        assert report.status == PipelineStatus.HEALTHY

    def test_report_warning_status(self):
        metric = make_metric(records_processed=100, records_failed=3)
        report = generate_report("pipe", metric, warn_threshold=0.02, error_threshold=0.05)
        assert report.status == PipelineStatus.WARNING

    def test_report_critical_status(self):
        metric = make_metric(records_processed=100, records_failed=10)
        report = generate_report("pipe", metric, error_threshold=0.05)
        assert report.status == PipelineStatus.CRITICAL

    def test_report_error_rate(self):
        metric = make_metric(records_processed=200, records_failed=10)
        report = generate_report("pipe", metric)
        assert report.error_rate == pytest.approx(0.05)

    def test_report_throughput(self):
        metric = make_metric(records_processed=50, duration_seconds=5.0)
        report = generate_report("pipe", metric)
        assert report.throughput == pytest.approx(10.0)

    def test_report_no_alerts_by_default(self):
        metric = make_metric()
        report = generate_report("pipe", metric)
        assert report.active_alerts == []

    def test_report_includes_active_alerts(self):
        metric = make_metric(records_processed=100, records_failed=10)
        alert = Alert(
            rule_name="HighErrorRate",
            severity=AlertSeverity.CRITICAL,
            message="Error rate exceeded threshold",
            triggered_at=datetime.utcnow(),
        )
        report = generate_report("pipe", metric, active_alerts=[alert])
        assert len(report.active_alerts) == 1
        assert report.active_alerts[0].rule_name == "HighErrorRate"


class TestPipelineReportSummary:
    def test_summary_contains_pipeline_id(self):
        metric = make_metric()
        report = generate_report("etl-main", metric)
        assert "etl-main" in report.summary()

    def test_summary_contains_status(self):
        metric = make_metric()
        report = generate_report("pipe", metric)
        assert "HEALTHY" in report.summary()

    def test_summary_contains_alert_info(self):
        metric = make_metric(records_processed=100, records_failed=20)
        alert = Alert(
            rule_name="HighErrorRate",
            severity=AlertSeverity.CRITICAL,
            message="Too many failures",
            triggered_at=datetime.utcnow(),
        )
        report = generate_report("pipe", metric, active_alerts=[alert])
        summary = report.summary()
        assert "HighErrorRate" in summary
        assert "Active Alerts: 1" in summary
