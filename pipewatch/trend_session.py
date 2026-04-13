"""Session-level trend analysis across registered pipelines."""

from typing import Dict, List, Optional
from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.trend_analyzer import (
    TrendResult,
    analyze_error_rate_trend,
    analyze_throughput_trend,
)


@dataclass_like = None  # avoid import cycle; use plain class


class TrendSession:
    """Run trend analysis for all pipelines in a registry."""

    def __init__(
        self,
        registry: PipelineRegistry,
        error_stable_threshold: float = 0.005,
        throughput_stable_threshold: float = 0.5,
    ) -> None:
        self._registry = registry
        self._error_threshold = error_stable_threshold
        self._throughput_threshold = throughput_stable_threshold

    def run(self) -> Dict[str, List[TrendResult]]:
        """Return a dict mapping pipeline_id to its trend results."""
        results: Dict[str, List[TrendResult]] = {}
        for pid in self._registry.pipeline_ids():
            collector = self._registry.collector_for(pid)
            if collector is None:
                continue
            metrics = collector.history()
            pipeline_results: List[TrendResult] = []
            err_trend = analyze_error_rate_trend(
                pid, metrics, self._error_threshold
            )
            if err_trend is not None:
                pipeline_results.append(err_trend)
            tp_trend = analyze_throughput_trend(
                pid, metrics, self._throughput_threshold
            )
            if tp_trend is not None:
                pipeline_results.append(tp_trend)
            results[pid] = pipeline_results
        return results

    def degrading_pipelines(self) -> List[str]:
        """Return pipeline IDs where any metric is degrading."""
        degrading = []
        for pid, trends in self.run().items():
            if any(t.direction == "degrading" for t in trends):
                degrading.append(pid)
        return degrading
