"""Tests for pipewatch.escalation."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.escalation import EscalationManager, EscalationPolicy


def make_alert(
    pipeline_id: str = "pipe-1",
    rule_name: str = "high_error_rate",
    severity: AlertSeverity = AlertSeverity.WARNING,
    message: str = "Error rate too high",
) -> Alert:
    return Alert(
        pipeline_id=pipeline_id,
        rule_name=rule_name,
        severity=severity,
        message=message,
        triggered_at=datetime.utcnow(),
    )


class TestEscalationManager:
    def setup_method(self):
        self.manager = EscalationManager()
        self.policy = EscalationPolicy(
            pipeline_id="pipe-1",
            rule_name="high_error_rate",
            escalate_after_seconds=300,
            escalate_to=AlertSeverity.CRITICAL,
        )
        self.manager.add_policy(self.policy)

    def test_first_alert_not_escalated(self):
        alert = make_alert()
        result = self.manager.evaluate(alert, now=datetime.utcnow())
        assert result.severity == AlertSeverity.WARNING

    def test_alert_not_escalated_before_window(self):
        now = datetime.utcnow()
        alert = make_alert()
        self.manager.evaluate(alert, now=now)
        result = self.manager.evaluate(alert, now=now + timedelta(seconds=100))
        assert result.severity == AlertSeverity.WARNING

    def test_alert_escalated_after_window(self):
        now = datetime.utcnow()
        alert = make_alert()
        self.manager.evaluate(alert, now=now)
        result = self.manager.evaluate(alert, now=now + timedelta(seconds=301))
        assert result.severity == AlertSeverity.CRITICAL

    def test_alert_escalated_at_exact_boundary(self):
        now = datetime.utcnow()
        alert = make_alert()
        self.manager.evaluate(alert, now=now)
        result = self.manager.evaluate(alert, now=now + timedelta(seconds=300))
        assert result.severity == AlertSeverity.CRITICAL

    def test_no_policy_returns_alert_unchanged(self):
        alert = make_alert(pipeline_id="other", rule_name="unknown")
        result = self.manager.evaluate(alert)
        assert result.severity == AlertSeverity.WARNING

    def test_clear_removes_state(self):
        now = datetime.utcnow()
        alert = make_alert()
        self.manager.evaluate(alert, now=now)
        assert "pipe-1:high_error_rate" in self.manager.active_keys()
        self.manager.clear("pipe-1", "high_error_rate")
        assert "pipe-1:high_error_rate" not in self.manager.active_keys()

    def test_after_clear_escalation_resets(self):
        now = datetime.utcnow()
        alert = make_alert()
        self.manager.evaluate(alert, now=now)
        self.manager.evaluate(alert, now=now + timedelta(seconds=400))
        self.manager.clear("pipe-1", "high_error_rate")
        # Re-evaluate as if alert fired again fresh
        result = self.manager.evaluate(alert, now=now + timedelta(seconds=500))
        assert result.severity == AlertSeverity.WARNING

    def test_active_keys_empty_initially(self):
        mgr = EscalationManager()
        assert mgr.active_keys() == []
