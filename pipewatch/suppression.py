"""Alert suppression rules to prevent alert fatigue by silencing repeated alerts."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


@dataclass
class SuppressionRule:
    """Defines a suppression window for a specific pipeline and rule combination."""

    pipeline_id: str
    rule_name: str
    duration_seconds: int

    def key(self) -> Tuple[str, str]:
        return (self.pipeline_id, self.rule_name)


@dataclass
class SuppressionStore:
    """Tracks active suppressions and determines whether alerts should be silenced."""

    _suppressed: Dict[Tuple[str, str], datetime] = field(default_factory=dict)

    def suppress(self, pipeline_id: str, rule_name: str, duration_seconds: int) -> None:
        """Mark an alert as suppressed until the duration expires."""
        key = (pipeline_id, rule_name)
        expiry = datetime.utcnow() + timedelta(seconds=duration_seconds)
        self._suppressed[key] = expiry

    def is_suppressed(self, pipeline_id: str, rule_name: str) -> bool:
        """Return True if the alert is currently suppressed."""
        key = (pipeline_id, rule_name)
        expiry = self._suppressed.get(key)
        if expiry is None:
            return False
        if datetime.utcnow() < expiry:
            return True
        del self._suppressed[key]
        return False

    def release(self, pipeline_id: str, rule_name: str) -> None:
        """Manually release a suppression before it expires."""
        key = (pipeline_id, rule_name)
        self._suppressed.pop(key, None)

    def active_suppressions(self) -> Dict[Tuple[str, str], datetime]:
        """Return a copy of currently active (non-expired) suppressions."""
        now = datetime.utcnow()
        return {k: v for k, v in self._suppressed.items() if v > now}

    def clear(self) -> None:
        """Remove all suppressions."""
        self._suppressed.clear()


def apply_suppression(
    store: SuppressionStore,
    pipeline_id: str,
    rule_name: str,
    auto_suppress_seconds: Optional[int] = None,
) -> bool:
    """
    Check if an alert should be suppressed. If not suppressed and
    auto_suppress_seconds is provided, register a new suppression.

    Returns True if the alert is suppressed (should be silenced).
    """
    if store.is_suppressed(pipeline_id, rule_name):
        return True
    if auto_suppress_seconds is not None and auto_suppress_seconds > 0:
        store.suppress(pipeline_id, rule_name, auto_suppress_seconds)
    return False
