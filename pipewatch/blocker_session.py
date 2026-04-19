from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_blocker import PipelineBlocker, BlockerResult


class BlockerSession:
    def __init__(self) -> None:
        self._blocker = PipelineBlocker()
        self._snapshots: Dict[str, PipelineSnapshot] = {}

    def block(self, pipeline_id: str, reason: str) -> None:
        self._blocker.block(pipeline_id, reason)

    def unblock(self, pipeline_id: str) -> bool:
        return self._blocker.unblock(pipeline_id)

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots[snapshot.pipeline_id] = snapshot

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> Optional[BlockerResult]:
        snapshots = list(self._snapshots.values())
        if not snapshots:
            return None
        return self._blocker.apply(snapshots)
