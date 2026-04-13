"""Metrics collector that aggregates and stores pipeline health snapshots."""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus


class MetricsCollector:
    """In-memory store for pipeline metrics with basic querying support."""

    def __init__(self, retention_hours: int = 24):
        self._store: Dict[str, List[PipelineMetric]] = defaultdict(list)
        self.retention_hours = retention_hours

    def record(self, metric: PipelineMetric) -> None:
        """Record a metric snapshot and prune old entries."""
        self._store[metric.pipeline_id].append(metric)
        self._prune(metric.pipeline_id)

    def latest(self, pipeline_id: str) -> Optional[PipelineMetric]:
        """Return the most recent metric for a given pipeline."""
        entries = self._store.get(pipeline_id)
        return entries[-1] if entries else None

    def history(self, pipeline_id: str, limit: int = 100) -> List[PipelineMetric]:
        """Return recent metric history for a pipeline, newest last."""
        entries = self._store.get(pipeline_id, [])
        return entries[-limit:]

    def all_pipeline_ids(self) -> List[str]:
        """Return list of all known pipeline IDs."""
        return list(self._store.keys())

    def summary(self) -> List[dict]:
        """Return latest status summary for all pipelines."""
        result = []
        for pid in self.all_pipeline_ids():
            metric = self.latest(pid)
            if metric:
                result.append(metric.to_dict())
        return result

    def unhealthy_pipelines(self) -> List[PipelineMetric]:
        """Return latest metrics for pipelines that are not healthy."""
        unhealthy = []
        for pid in self.all_pipeline_ids():
            metric = self.latest(pid)
            if metric and metric.status not in (PipelineStatus.HEALTHY, PipelineStatus.UNKNOWN):
                unhealthy.append(metric)
        return unhealthy

    def _prune(self, pipeline_id: str) -> None:
        """Remove entries older than the retention window."""
        cutoff = datetime.utcnow() - timedelta(hours=self.retention_hours)
        self._store[pipeline_id] = [
            m for m in self._store[pipeline_id] if m.timestamp >= cutoff
        ]
