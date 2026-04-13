"""Orchestrate drift detection across all tracked pipelines in a WatchSession."""

from typing import Dict, List

from pipewatch.baseline_store import BaselineStore
from pipewatch.drift_detector import DriftResult, detect_drift
from pipewatch.snapshot import capture_snapshot
from pipewatch.watch_session import WatchSession


class DriftSession:
    """Wraps a WatchSession and a BaselineStore to produce drift reports."""

    def __init__(
        self,
        watch_session: WatchSession,
        baseline_store: BaselineStore,
        error_rate_threshold: float = 0.05,
        throughput_threshold: float = 0.20,
    ) -> None:
        self._session = watch_session
        self._baselines = baseline_store
        self._er_threshold = error_rate_threshold
        self._tp_threshold = throughput_threshold

    def run(self) -> List[DriftResult]:
        """Capture current snapshots and compare against stored baselines.

        Pipelines without a baseline are skipped.
        """
        results: List[DriftResult] = []
        for pid in self._session.pipeline_ids():
            baseline = self._baselines.get(pid)
            if baseline is None:
                continue
            collector = self._session._registry.collector_for(pid)
            metrics = collector.history()
            snapshot = capture_snapshot(pid, metrics)
            if snapshot is None:
                continue
            result = detect_drift(
                baseline,
                snapshot,
                error_rate_threshold=self._er_threshold,
                throughput_threshold=self._tp_threshold,
            )
            results.append(result)
        return results

    def set_baseline_from_current(self, pipeline_id: str) -> bool:
        """Snapshot current metrics and store as baseline. Returns False if no data."""
        collector = self._session._registry.collector_for(pipeline_id)
        if collector is None:
            return False
        metrics = collector.history()
        snapshot = capture_snapshot(pipeline_id, metrics)
        if snapshot is None:
            return False
        self._baselines.set(snapshot)
        return True
