"""Session that wires alert manager output to notification routing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List

from pipewatch.alert_manager import AlertManager
from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric
from pipewatch.notification import NotificationChannel, NotificationRouter


@dataclass
class NotificationSession:
    """Combines an AlertManager with a NotificationRouter to evaluate and deliver alerts."""

    alert_manager: AlertManager
    router: NotificationRouter = field(default_factory=NotificationRouter)
    _dispatched: List[Dict] = field(default_factory=list, init=False)

    def add_channel(
        self,
        name: str,
        handler: Callable[[Alert], None],
        min_severity: str = "warning",
    ) -> None:
        """Register a notification channel by name and handler."""
        self.router.add_channel(
            NotificationChannel(name=name, handler=handler, min_severity=min_severity)
        )

    def run(self, metrics: List[PipelineMetric]) -> List[Dict]:
        """Evaluate metrics for alerts and dispatch notifications. Returns dispatch records."""
        alerts = self.alert_manager.check(metrics)
        records = []
        for alert in alerts:
            notified = self.router.dispatch(alert)
            record = {
                "pipeline_id": alert.pipeline_id,
                "rule_name": alert.rule_name,
                "severity": alert.severity.value,
                "message": alert.message,
                "channels_notified": notified,
            }
            records.append(record)
        self._dispatched.extend(records)
        return records

    @property
    def total_dispatched(self) -> int:
        """Total number of alert dispatches across all run() calls."""
        return len(self._dispatched)

    def history(self) -> List[Dict]:
        """Return all dispatch records accumulated across run() calls."""
        return list(self._dispatched)

    def clear_history(self) -> None:
        """Clear all accumulated dispatch records."""
        self._dispatched.clear()

    def history_for_pipeline(self, pipeline_id: str) -> List[Dict]:
        """Return dispatch records filtered to a specific pipeline ID."""
        return [r for r in self._dispatched if r["pipeline_id"] == pipeline_id]
