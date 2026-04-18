from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PinEntry:
    pipeline_id: str
    pinned_at: datetime
    reason: str

    def __str__(self) -> str:
        return f"PinEntry({self.pipeline_id}, reason={self.reason!r}, pinned_at={self.pinned_at.isoformat()})"


@dataclass
class PinnerResult:
    pinned: List[str]
    unpinned: List[str]

    @property
    def total_pinned(self) -> int:
        return len(self.pinned)

    @property
    def total_unpinned(self) -> int:
        return len(self.unpinned)


class PipelinePinner:
    def __init__(self) -> None:
        self._pins: Dict[str, PinEntry] = {}

    def pin(self, pipeline_id: str, reason: str = "", pinned_at: Optional[datetime] = None) -> PinEntry:
        entry = PinEntry(
            pipeline_id=pipeline_id,
            pinned_at=pinned_at or datetime.utcnow(),
            reason=reason,
        )
        self._pins[pipeline_id] = entry
        return entry

    def unpin(self, pipeline_id: str) -> bool:
        if pipeline_id in self._pins:
            del self._pins[pipeline_id]
            return True
        return False

    def is_pinned(self, pipeline_id: str) -> bool:
        return pipeline_id in self._pins

    def pinned_ids(self) -> List[str]:
        return list(self._pins.keys())

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> PinnerResult:
        pinned = [s.pipeline_id for s in snapshots if self.is_pinned(s.pipeline_id)]
        unpinned = [s.pipeline_id for s in snapshots if not self.is_pinned(s.pipeline_id)]
        return PinnerResult(pinned=pinned, unpinned=unpinned)
