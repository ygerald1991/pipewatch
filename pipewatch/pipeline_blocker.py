from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class BlockEntry:
    pipeline_id: str
    reason: str
    blocked_at: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        return f"BlockEntry({self.pipeline_id}, reason={self.reason!r})"


@dataclass
class BlockerResult:
    blocked: List[BlockEntry]
    allowed: List[PipelineSnapshot]

    @property
    def total_blocked(self) -> int:
        return len(self.blocked)

    @property
    def total_allowed(self) -> int:
        return len(self.allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        blocked_ids = [e.pipeline_id for e in self.blocked]
        allowed_ids = [s.pipeline_id for s in self.allowed]
        return blocked_ids + allowed_ids


class PipelineBlocker:
    def __init__(self) -> None:
        self._blocked: Dict[str, BlockEntry] = {}

    def block(self, pipeline_id: str, reason: str) -> BlockEntry:
        entry = BlockEntry(pipeline_id=pipeline_id, reason=reason)
        self._blocked[pipeline_id] = entry
        return entry

    def unblock(self, pipeline_id: str) -> bool:
        if pipeline_id in self._blocked:
            del self._blocked[pipeline_id]
            return True
        return False

    def is_blocked(self, pipeline_id: str) -> bool:
        return pipeline_id in self._blocked

    def entry_for(self, pipeline_id: str) -> Optional[BlockEntry]:
        return self._blocked.get(pipeline_id)

    def apply(self, snapshots: List[PipelineSnapshot]) -> BlockerResult:
        blocked: List[BlockEntry] = []
        allowed: List[PipelineSnapshot] = []
        for snapshot in snapshots:
            entry = self._blocked.get(snapshot.pipeline_id)
            if entry:
                blocked.append(entry)
            else:
                allowed.append(snapshot)
        return BlockerResult(blocked=blocked, allowed=allowed)

    @property
    def blocked_pipeline_ids(self) -> List[str]:
        return list(self._blocked.keys())
