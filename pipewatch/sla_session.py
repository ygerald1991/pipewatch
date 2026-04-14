"""Orchestrates SLA evaluation across all registered pipelines."""

from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.sla_tracker import SLAPolicy, SLAResult, evaluate_sla


class SLASession:
    """Run SLA checks for every pipeline tracked in a registry."""

    def __init__(
        self,
        registry: PipelineRegistry,
        policy: Optional[SLAPolicy] = None,
    ) -> None:
        self._registry = registry
        self._policy = policy or SLAPolicy()
        self._results: list[SLAResult] = []

    # ------------------------------------------------------------------
    def run(self) -> list[SLAResult]:
        """Evaluate SLA for all pipelines and cache the results."""
        self._results = []
        for pid in self._registry.pipeline_ids():
            collector = self._registry.collector_for(pid)
            metrics = collector.history() if collector else []
            result = evaluate_sla(pid, metrics, self._policy)
            if result is not None:
                self._results.append(result)
        return self._results

    # ------------------------------------------------------------------
    def breaching_pipelines(self) -> list[SLAResult]:
        """Return only the results where the SLA was *not* met."""
        return [r for r in self._results if not r.met]

    def all_results(self) -> list[SLAResult]:
        return list(self._results)

    def pipeline_ids(self) -> list[str]:
        return [r.pipeline_id for r in self._results]
