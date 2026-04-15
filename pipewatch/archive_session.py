"""Session that drives snapshot archiving across all registered pipelines."""

from __future__ import annotations

from typing import Dict, List, Optional

from pipewatch.pipeline_archiver import ArchiveEntry, ArchiveStore
from pipewatch.snapshot import PipelineSnapshot, capture_snapshot
from pipewatch.collector import MetricsCollector


class ArchiveSession:
    """Captures and archives snapshots for a collection of pipelines."""

    def __init__(self, store: Optional[ArchiveStore] = None, max_entries: int = 50) -> None:
        self._store = store or ArchiveStore(max_entries_per_pipeline=max_entries)
        self._collectors: Dict[str, MetricsCollector] = {}

    def register(self, pipeline_id: str, collector: MetricsCollector) -> None:
        self._collectors[pipeline_id] = collector

    def run(self) -> Dict[str, ArchiveEntry]:
        """Archive the current snapshot for every registered pipeline.

        Returns a mapping of pipeline_id -> ArchiveEntry for pipelines
        that had sufficient metrics to produce a snapshot.
        """
        results: Dict[str, ArchiveEntry] = {}
        for pid, collector in self._collectors.items():
            snapshot = capture_snapshot(pid, collector)
            if snapshot is not None:
                entry = self._store.archive(snapshot)
                results[pid] = entry
        return results

    def pipeline_ids(self) -> List[str]:
        return list(self._collectors.keys())

    @property
    def store(self) -> ArchiveStore:
        return self._store
