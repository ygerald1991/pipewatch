"""Registry that tracks known IDs and their collectors."""

from typing import Dict, List, Optional

from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric
Registry:
    """Central registry mapping pipeline IDs to their MetricsCollector."""

    def __init__(self) -> None:
        self._collectors: Dict[str, MetricsCollector] = {}

    def register(self, pipeline_id: str) -> MetricsCollector:
        """Register a pipeline and return its collector (idempotent)."""
        if pipeline_id not in self._collectors:
            self._collectors[pipeline_id] = MetricsCollector()
        return self._collectors[pipeline_id]

    def record(self, metric: PipelineMetric) -> None:
        """Record a metric, auto-registering the pipeline if needed."""
        collector = self.register(metric.pipeline_id)
        collector.record(metric)

    def collector_for(self, pipeline_id: str) -> Optional[MetricsCollector]:
        return self._collectors.get(pipeline_id)

    def pipeline_ids(self) -> List[str]:
        return list(self._collectors.keys())

    def __len__(self) -> int:
        return len(self._collectors)

    def __contains__(self, pipeline_id: str) -> bool:
        return pipeline_id in self._collectors
