"""Escalation policy: promote alerts to higher severity after a cooldown period."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class EscalationPolicy:
    """Defines when and how an alert should be escalated."""

    pipeline_id: str
    rule_name: str
    escalate_after_seconds: int = 300  # 5 minutes default
    escalate_to: AlertSeverity = AlertSeverity.CRITICAL

    @property
    def key(self) -> str:
        return f"{self.pipeline_id}:{self.rule_name}"


@dataclass
class EscalationState:
    first_seen: datetime
    current_severity: AlertSeverity
    escalated: bool = False


class EscalationManager:
    """Tracks active alerts and escalates them if they persist beyond the policy window."""

    def __init__(self) -> None:
        self._policies: Dict[str, EscalationPolicy] = {}
        self._states: Dict[str, EscalationState] = {}

    def add_policy(self, policy: EscalationPolicy) -> None:
        self._policies[policy.key] = policy

    def evaluate(self, alert: Alert, now: Optional[datetime] = None) -> Alert:
        """Return the alert, potentially with an escalated severity."""
        now = now or datetime.utcnow()
        key = f"{alert.pipeline_id}:{alert.rule_name}"
        policy = self._policies.get(key)

        if policy is None:
            return alert

        if key not in self._states:
            self._states[key] = EscalationState(
                first_seen=now, current_severity=alert.severity
            )
            return alert

        state = self._states[key]
        elapsed = (now - state.first_seen).total_seconds()

        if not state.escalated and elapsed >= policy.escalate_after_seconds:
            state.escalated = True
            state.current_severity = policy.escalate_to

        return Alert(
            pipeline_id=alert.pipeline_id,
            rule_name=alert.rule_name,
            severity=state.current_severity,
            message=alert.message,
            triggered_at=alert.triggered_at,
        )

    def clear(self, pipeline_id: str, rule_name: str) -> None:
        """Remove escalation state when an alert resolves."""
        key = f"{pipeline_id}:{rule_name}"
        self._states.pop(key, None)

    def active_keys(self) -> list:
        return list(self._states.keys())
