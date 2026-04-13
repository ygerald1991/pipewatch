"""Wires EscalationManager into the alert pipeline for a watch session."""

from __future__ import annotations

from typing import List

from pipewatch.alert_manager import AlertManager
from pipewatch.alerts import Alert
from pipewatch.escalation import EscalationManager, EscalationPolicy
from pipewatch.watch_session import WatchSession


class EscalationSession:
    """Runs alert checks and applies escalation policies to the results."""

    def __init__(
        self,
        watch_session: WatchSession,
        alert_manager: AlertManager,
        escalation_manager: EscalationManager,
    ) -> None:
        self._watch = watch_session
        self._alert_manager = alert_manager
        self._escalation = escalation_manager

    def add_escalation_policy(self, policy: EscalationPolicy) -> None:
        self._escalation.add_policy(policy)

    def run(self) -> List[Alert]:
        """Check all pipelines and return escalation-aware alerts."""
        raw_alerts: List[Alert] = []

        for pipeline_id in self._watch.pipeline_ids():
            collector = self._watch._registry.collector_for(pipeline_id)
            if collector is None:
                continue
            metrics = collector.history()
            if not metrics:
                continue
            latest = collector.latest()
            if latest is None:
                continue
            triggered = self._alert_manager.check(latest)
            raw_alerts.extend(triggered)

        escalated: List[Alert] = []
        seen_keys = set()

        for alert in raw_alerts:
            result = self._escalation.evaluate(alert)
            escalated.append(result)
            seen_keys.add(f"{alert.pipeline_id}:{alert.rule_name}")

        # Clear escalation state for pipelines no longer alerting
        for key in list(self._escalation.active_keys()):
            if key not in seen_keys:
                pipeline_id, rule_name = key.split(":", 1)
                self._escalation.clear(pipeline_id, rule_name)

        return escalated

    @property
    def pipeline_ids(self) -> List[str]:
        return self._watch.pipeline_ids()
