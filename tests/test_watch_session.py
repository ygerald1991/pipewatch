"""Tests for WatchSession."""

from datetime import datetime

from pipewatch.watch_session import WatchSession
from pipewatch.metrics import PipelineMetric
from pipewatch.alert_manager import AlertManager
from pipewatch.alerts import AlertRule, AlertSeverity


def make_metric(pipeline_id="pipe-1", processed=200, failed=1, duration=60.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
    )


class TestWatchSession:
    def test_ingest_registers_pipeline(self):
        session = WatchSession()
        session.ingest(make_metric("pipe-1"))
        assert "pipe-1" in session.pipeline_ids()

    def test_run_checks_returns_result_per_pipeline(self):
        session = WatchSession()
        session.ingest(make_metric("pipe-1"))
        session.ingest(make_metric("pipe-2"))
        results = session.run_checks()
        assert set(results.keys()) == {"pipe-1", "pipe-2"}

    def test_run_checks_empty_session_returns_empty(self):
        session = WatchSession()
        results = session.run_checks()
        assert results == {}

    def test_healthy_result_when_no_alerts(self):
        session = WatchSession()
        session.ingest(make_metric("pipe-1", failed=0))
        results = session.run_checks()
        assert results["pipe-1"].healthy

    def test_alerts_fired_counted_in_result(self):
        rule = AlertRule(
            name="high-errors",
            metric="error_rate",
            threshold=0.01,
            severity=AlertSeverity.CRITICAL,
        )
        am = AlertManager()
        am.add_rule(rule)
        session = WatchSession(alert_manager=am)
        # 50% error rate — well above 1%
        session.ingest(make_metric("pipe-1", processed=100, failed=50))
        results = session.run_checks()
        assert results["pipe-1"].alerts_fired > 0

    def test_from_config_creates_session(self):
        session = WatchSession.from_config({})
        assert isinstance(session, WatchSession)
