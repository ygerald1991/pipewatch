"""Session that runs correlation analysis across all registered pipelines."""

from __future__ import annotations

from itertools import combinations
from typing import Dict, List, Optional

from pipewatch.correlation import CorrelationResult, correlate_pipelines
from pipewatch.snapshot import PipelineSnapshot

_DEFAULT_METRICS = ["error_rate", "throughput"]


class CorrelationSession:
    """Holds snapshot histories per pipeline and runs pairwise correlation."""

    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        significance_threshold: float = 0.7,
    ) -> None:
        self._metrics = metrics or _DEFAULT_METRICS
        self._threshold = significance_threshold
        self._histories: Dict[str, List[PipelineSnapshot]] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        self._histories.setdefault(pid, []).append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return list(self._histories.keys())

    def run(self) -> List[CorrelationResult]:
        """Return correlation results for every unique pipeline pair."""
        ids = self.pipeline_ids()
        results: List[CorrelationResult] = []
        for id_a, id_b in combinations(ids, 2):
            pair_results = correlate_pipelines(
                self._histories[id_a],
                self._histories[id_b],
                self._metrics,
                significance_threshold=self._threshold,
            )
            results.extend(pair_results)
        return results

    def significant_results(self) -> List[CorrelationResult]:
        return [r for r in self.run() if r.is_significant]
