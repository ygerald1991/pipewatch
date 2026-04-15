from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.quota_tracker import QuotaPolicy, QuotaResult, evaluate_quota


class QuotaSession:
    """Evaluates throughput quotas across all registered pipelines."""

    def __init__(self, registry: PipelineRegistry, policy: Optional[QuotaPolicy] = None) -> None:
        self._registry = registry
        self._policy = policy or QuotaPolicy()
        self._results: List[QuotaResult] = []

    @property
    def policy(self) -> QuotaPolicy:
        return self._policy

    def run(self) -> List[QuotaResult]:
        """Run quota evaluation for every registered pipeline."""
        results: List[QuotaResult] = []
        for pid in self._registry.pipeline_ids():
            collector = self._registry.collector_for(pid)
            metrics = collector.history(pid) if collector else []
            result = evaluate_quota(pid, metrics, self._policy)
            if result is not None:
                results.append(result)
        self._results = results
        return results

    def exceeding_pipelines(self) -> List[QuotaResult]:
        """Return only pipelines that have exceeded their quota."""
        return [r for r in self._results if r.exceeded]

    def utilization_map(self) -> Dict[str, float]:
        """Return a mapping of pipeline_id -> utilization ratio."""
        return {r.pipeline_id: r.utilization for r in self._results}
