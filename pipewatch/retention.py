"""Metric retention policy: prune old metrics from collector history."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict

from pipewatch.collector import MetricsCollector


@dataclass
class RetentionPolicy:
    """Defines how long metrics are retained per pipeline (or globally)."""

    default_max_age_hours: float = 24.0
    pipeline_overrides: Dict[str, float] = field(default_factory=dict)

    def max_age_for(self, pipeline_id: str) -> timedelta:
        hours = self.pipeline_overrides.get(pipeline_id, self.default_max_age_hours)
        return timedelta(hours=hours)


def prune_collector(
    collector: MetricsCollector,
    policy: RetentionPolicy,
    now: datetime | None = None,
) -> int:
    """Remove metrics older than the retention window from *collector*.

    Returns the number of metrics pruned.
    """
    if now is None:
        now = datetime.utcnow()

    pipeline_id = collector.pipeline_id
    cutoff = now - policy.max_age_for(pipeline_id)

    before = len(collector._history)  # type: ignore[attr-defined]
    collector._history = [
        m for m in collector._history if m.timestamp >= cutoff  # type: ignore[attr-defined]
    ]
    after = len(collector._history)  # type: ignore[attr-defined]
    return before - after


def apply_retention(
    collectors: Dict[str, MetricsCollector],
    policy: RetentionPolicy,
    now: datetime | None = None,
) -> Dict[str, int]:
    """Apply retention policy across all collectors.

    Returns a mapping of pipeline_id -> pruned count.
    """
    return {
        pid: prune_collector(collector, policy, now=now)
        for pid, collector in collectors.items()
    }
