"""High-level watch session that ties registry, alerts, and runner together."""

from typing import Dict, List, Optional

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.pipeline_runner import RunResult, run_pipeline_check
from pipewatch.alert_manager import AlertManager
from pipewatch.thresholds import ThresholdConfig, load_thresholds
from pipewatch.metrics import PipelineMetric


class WatchSession:
    """Coordinates a monitoring session across multiple pipelines."""

    def __init__(
        self,
        alert_manager: Optional[AlertManager] = None,
        threshold_config: Optional[ThresholdConfig] = None,
    ) -> None:
        self.registry = PipelineRegistry()
        self.alert_manager = alert_manager or AlertManager()
        self.threshold_config = threshold_config

    def ingest(self, metric: PipelineMetric) -> None:
        """Ingest a metric into the session registry."""
        self.registry.record(metric)

    def run_checks(self) -> Dict[str, RunResult]:
        """Run health checks for every registered pipeline."""
        results: Dict[str, RunResult] = {}
        for pid in self.registry.pipeline_ids():
            collector = self.registry.collector_for(pid)
            if collector is None:
                continue
            results[pid] = run_pipeline_check(
                pid, collector, self.alert_manager, self.threshold_config
            )
        return results

    def pipeline_ids(self) -> List[str]:
        return self.registry.pipeline_ids()

    @classmethod
    def from_config(cls, config: dict) -> "WatchSession":
        """Build a WatchSession from a config dict."""
        raw_thresholds = config.get("thresholds")
        tc = load_thresholds(raw_thresholds) if raw_thresholds else None
        return cls(threshold_config=tc)
