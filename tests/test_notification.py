"""Tests for notification channel routing."""

import pytest
from unittest.mock import MagicMock

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.notification import NotificationChannel, NotificationRouter


def make_alert(severity: str = "warning", pipeline_id: str = "pipe-1", rule_name: str = "high_error_rate") -> Alert:
    return Alert(
        pipeline_id=pipeline_id,
        rule_name=rule_name,
        severity=AlertSeverity(severity),
        message=f"Test alert [{severity}] for {pipeline_id}",
    )


class TestNotificationChannel:
    def test_send_returns_true_when_severity_met(self):
        handler = MagicMock()
        channel = NotificationChannel(name="slack", handler=handler, min_severity="warning")
        alert = make_alert(severity="warning")
        assert channel.send(alert) is True
        handler.assert_called_once_with(alert)

    def test_send_returns_false_when_severity_too_low(self):
        handler = MagicMock()
        channel = NotificationChannel(name="pagerduty", handler=handler, min_severity="critical")
        alert = make_alert(severity="warning")
        assert channel.send(alert) is False
        handler.assert_not_called()

    def test_critical_alert_reaches_warning_channel(self):
        handler = MagicMock()
        channel = NotificationChannel(name="email", handler=handler, min_severity="warning")
        alert = make_alert(severity="critical")
        assert channel.send(alert) is True

    def test_info_alert_blocked_by_warning_channel(self):
        handler = MagicMock()
        channel = NotificationChannel(name="log", handler=handler, min_severity="warning")
        alert = make_alert(severity="info")
        assert channel.send(alert) is False

    def test_should_notify_false_for_unknown_severity(self):
        handler = MagicMock()
        channel = NotificationChannel(name="test", handler=handler, min_severity="unknown")
        alert = make_alert(severity="warning")
        assert channel.should_notify(alert) is False


class TestNotificationRouter:
    def test_dispatch_notifies_eligible_channels(self):
        handler_a = MagicMock()
        handler_b = MagicMock()
        router = NotificationRouter()
        router.add_channel(NotificationChannel("slack", handler_a, min_severity="warning"))
        router.add_channel(NotificationChannel("pagerduty", handler_b, min_severity="critical"))
        alert = make_alert(severity="warning")
        notified = router.dispatch(alert)
        assert "slack" in notified
        assert "pagerduty" not in notified
        handler_a.assert_called_once()
        handler_b.assert_not_called()

    def test_dispatch_returns_empty_when_no_channels(self):
        router = NotificationRouter()
        alert = make_alert(severity="critical")
        assert router.dispatch(alert) == []

    def test_dispatch_all_keys_by_pipeline_and_rule(self):
        handler = MagicMock()
        router = NotificationRouter()
        router.add_channel(NotificationChannel("email", handler, min_severity="info"))
        alerts = [
            make_alert(severity="warning", pipeline_id="p1", rule_name="err_rate"),
            make_alert(severity="critical", pipeline_id="p2", rule_name="throughput"),
        ]
        results = router.dispatch_all(alerts)
        assert "p1:err_rate" in results
        assert "p2:throughput" in results
        assert results["p1:err_rate"] == ["email"]

    def test_dispatch_all_empty_alerts_returns_empty(self):
        router = NotificationRouter()
        assert router.dispatch_all([]) == {}
