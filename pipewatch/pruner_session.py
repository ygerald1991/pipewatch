from __future__ import annotations
from typing import List, Dict
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_pruner import PrunerResult, prune_snapshots


class PrunerSession:
    def __init__(self, max_per_pipeline: int = 10) -> None:
        self._max = max_per_pipeline
        self._snapshots: List[PipelineSnapshot] = []
        self._last_result: PrunerResult | None = None

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def pipeline_ids(self) -> List[str]:
        seen = []
        for s in self._snapshots:
            if s.pipeline_id not in seen:
                seen.append(s.pipeline_id)
        return seen

    def run(self) -> PrunerResult:
        self._last_result = prune_snapshots(self._snapshots, self._max)
        return self._last_result

    @property
    def last_result(self) -> PrunerResult | None:
        return self._last_result
