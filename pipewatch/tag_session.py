"""Session that runs pipeline checks scoped to a TagFilter."""
from __future__ import annotations
from typing import Dict, List, Optional

from pipewatch.tag_filter import TagFilter, TagRegistry
from pipewatch.watch_session import WatchSession
from pipewatch.pipeline_runner import RunResult
from pipewatch.metrics import PipelineMetric
from pipewatch.thresholds import ThresholdConfig


class TagSession:
    """Wraps a WatchSession and restricts execution to tagged pipelines."""

    def __init__(
        self,
        registry: TagRegistry,
        tag_filter: TagFilter,
        thresholds: Optional[ThresholdConfig] = None,
    ) -> None:
        self._registry = registry
        self._filter = tag_filter
        self._session = WatchSession(thresholds=thresholds)

    def ingest(self, metric: PipelineMetric) -> None:
        """Ingest *metric* regardless of tags; filtering happens at run time."""
        self._session.ingest(metric)

    def run_checks(self) -> Dict[str, RunResult]:
        """Run checks only for pipelines that match the tag filter."""
        all_results = self._session.run_checks()
        allowed = set(
            self._registry.filter_pipelines(
                self._filter, candidates=list(all_results.keys())
            )
        )
        return {pid: result for pid, result in all_results.items() if pid in allowed}

    def pipeline_ids(self) -> List[str]:
        """Return pipeline IDs currently tracked by the inner session."""
        return self._session.pipeline_ids()

    def matched_pipeline_ids(self) -> List[str]:
        """Return only the pipeline IDs that pass the tag filter."""
        return self._registry.filter_pipelines(
            self._filter, candidates=self.pipeline_ids()
        )
