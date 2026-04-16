from __future__ import annotations
from typing import List, Optional

from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_sampler import SampleResult, sample_snapshots


class SamplerSession:
    def __init__(self, n: int = 10, seed: Optional[int] = None) -> None:
        self._n = n
        self._seed = seed
        self._snapshots: List[PipelineSnapshot] = []

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    @property
    def pipeline_ids(self) -> List[str]:
        seen: dict[str, None] = {}
        for s in self._snapshots:
            seen[s.pipeline_id] = None
        return list(seen)

    def run(self) -> List[SampleResult]:
        return sample_snapshots(self._snapshots, self._n, seed=self._seed)

    def total_snapshots(self) -> int:
        return len(self._snapshots)
