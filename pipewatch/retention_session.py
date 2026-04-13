"""High-level session that wires retention into a pipeline registry."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.retention import RetentionPolicy, apply_retention


class RetentionSession:
    """Applies a :class:`RetentionPolicy` to all pipelines in a registry."""

    def __init__(
        self,
        registry: PipelineRegistry,
        policy: Optional[RetentionPolicy] = None,
    ) -> None:
        self._registry = registry
        self._policy = policy or RetentionPolicy()
        self._last_run: Optional[datetime] = None

    @property
    def policy(self) -> RetentionPolicy:
        return self._policy

    def run(self, now: Optional[datetime] = None) -> Dict[str, int]:
        """Prune stale metrics and return per-pipeline pruned counts."""
        now = now or datetime.utcnow()
        collectors = {
            pid: self._registry.collector_for(pid)
            for pid in self._registry.pipeline_ids()
        }
        result = apply_retention(collectors, self._policy, now=now)
        self._last_run = now
        return result

    @property
    def last_run(self) -> Optional[datetime]:
        return self._last_run

    def total_pruned(self, result: Dict[str, int]) -> int:
        return sum(result.values())
