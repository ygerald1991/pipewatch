from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class FreezeEntry:
    pipeline_id: str
    frozen_at: datetime
    reason: str
    snapshot: PipelineSnapshot

    def __str__(self) -> str:
        return f"FreezeEntry({self.pipeline_id}, frozen_at={self.frozen_at.isoformat()}, reason={self.reason!r})"


@dataclass
class FreezerResult:
    frozen: List[FreezeEntry] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def total_frozen(self) -> int:
        return len(self.frozen)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.frozen]

    def __str__(self) -> str:
        return f"FreezerResult(frozen={self.total_frozen}, skipped={len(self.skipped)})"


class PipelineFreezer:
    def __init__(self) -> None:
        self._entries: Dict[str, FreezeEntry] = {}

    def freeze(self, snapshot: PipelineSnapshot, reason: str = "manual") -> FreezeEntry:
        entry = FreezeEntry(
            pipeline_id=snapshot.pipeline_id,
            frozen_at=datetime.utcnow(),
            reason=reason,
            snapshot=snapshot,
        )
        self._entries[snapshot.pipeline_id] = entry
        return entry

    def unfreeze(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def is_frozen(self, pipeline_id: str) -> bool:
        return pipeline_id in self._entries

    def frozen_entry(self, pipeline_id: str) -> Optional[FreezeEntry]:
        return self._entries.get(pipeline_id)

    def freeze_snapshots(
        self, snapshots: List[PipelineSnapshot], reason: str = "bulk"
    ) -> FreezerResult:
        result = FreezerResult()
        for snap in snapshots:
            if self.is_frozen(snap.pipeline_id):
                result.skipped.append(snap.pipeline_id)
            else:
                entry = self.freeze(snap, reason=reason)
                result.frozen.append(entry)
        return result

    @property
    def all_frozen(self) -> List[FreezeEntry]:
        return list(self._entries.values())
