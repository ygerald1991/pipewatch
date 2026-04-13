"""Notification channel abstractions for delivering alerts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List

from pipewatch.alerts import Alert


@dataclass
class NotificationChannel:
    """A named channel that delivers alerts via a handler function."""

    name: str
    handler: Callable[[Alert], None]
    min_severity: str = "warning"

    _SEVERITY_ORDER = ["info", "warning", "critical"]

    def should_notify(self, alert: Alert) -> bool:
        """Return True if the alert meets the minimum severity threshold."""
        try:
            alert_level = self._SEVERITY_ORDER.index(alert.severity.value)
            min_level = self._SEVERITY_ORDER.index(self.min_severity)
        except ValueError:
            return False
        return alert_level >= min_level

    def send(self, alert: Alert) -> bool:
        """Send the alert if it meets severity requirements. Returns True if sent."""
        if self.should_notify(alert):
            self.handler(alert)
            return True
        return False


@dataclass
class NotificationRouter:
    """Routes alerts to one or more notification channels."""

    channels: List[NotificationChannel] = field(default_factory=list)

    def add_channel(self, channel: NotificationChannel) -> None:
        """Register a notification channel."""
        self.channels.append(channel)

    def dispatch(self, alert: Alert) -> List[str]:
        """Dispatch an alert to all eligible channels. Returns list of channel names notified."""
        notified = []
        for channel in self.channels:
            if channel.send(alert):
                notified.append(channel.name)
        return notified

    def dispatch_all(self, alerts: List[Alert]) -> dict:
        """Dispatch multiple alerts. Returns mapping of alert pipeline_id to notified channels."""
        results = {}
        for alert in alerts:
            key = f"{alert.pipeline_id}:{alert.rule_name}"
            results[key] = self.dispatch(alert)
        return results
