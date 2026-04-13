"""Session wrapper that runs anomaly detection across all registered pipelines."""

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.anomaly_detector import AnomalyResult, analyze_metrics_for_anomalies


@dataclass
class AnomalySession:
    registry: PipelineRegistry
    z_threshold: float = 2.5
    _results: Dict[str, List[AnomalyResult]] = field(default_factory=dict, init=False)

    def run(self) -> Dict[str, List[AnomalyResult]]:
        """Analyse every known pipeline and return a mapping of pipeline_id -> results."""
        self._results = {}
        for pid in self.registry.pipeline_ids():
            collector = self.registry.collector_for(pid)
            if collector is None:
                continue
            metrics = collector.history()
            results = analyze_metrics_for_anomalies(pid, metrics, self.z_threshold)
            self._results[pid] = results
        return self._results

    def anomalous_pipelines(self) -> List[str]:
        """Return pipeline IDs that have at least one anomalous metric."""
        return [
            pid
            for pid, results in self._results.items()
            if any(r.is_anomaly for r in results)
        ]

    def all_results(self) -> List[AnomalyResult]:
        """Flatten all results across all pipelines."""
        flat: List[AnomalyResult] = []
        for results in self._results.values():
            flat.extend(results)
        return flat
