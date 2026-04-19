from __future__ import annotations
from typing import List, Optional
from pipewatch.pipeline_blacklister import PipelineBlacklister, BlacklisterResult
from pipewatch.snapshot import PipelineSnapshot


class BlacklisterSession:
    def __init__(self) -> None:
        self._blacklister = PipelineBlacklister()
        self._snapshots: List[PipelineSnapshot] = []

    def blacklist(self, pipeline_id: str, reason: str = "") -> None:
        self._blacklister.blacklist(pipeline_id, reason)

    def unblacklist(self, pipeline_id: str) -> bool:
        return self._blacklister.unblacklist(pipeline_id)

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    @property
    def pipeline_ids(self) -> List[str]:
        return list({s.pipeline_id for s in self._snapshots})

    def run(self) -> Optional[BlacklisterResult]:
        if not self._snapshots:
            return None
        return self._blacklister.apply(self._snapshots)
