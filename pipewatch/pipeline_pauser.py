from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PauseEntry:
    pipeline_id: str
    paused_at: datetime
    reason: str
    resumed_at: Optional[datetime] = None

    @property
    def is_active(self) -> bool:
        return self.resumed_at is None

    def __str__(self) -> str:
        status = "paused" if self.is_active else "resumed"
        return f"{self.pipeline_id} [{status}] since {self.paused_at.isoformat()}"


@dataclass
class PauserResult:
    paused: List[str] = field(default_factory=list)
    active: List[str] = field(default_factory=list)

    @property
    def total_paused(self) -> int:
        return len(self.paused)


class PipelinePauser:
    def __init__(self) -> None:
        self._entries: Dict[str, PauseEntry] = {}

    def pause(self, pipeline_id: str, reason: str = "") -> PauseEntry:
        entry = PauseEntry(
            pipeline_id=pipeline_id,
            paused_at=datetime.now(timezone.utc),
            reason=reason,
        )
        self._entries[pipeline_id] = entry
        return entry

    def resume(self, pipeline_id: str) -> Optional[PauseEntry]:
        entry = self._entries.get(pipeline_id)
        if entry and entry.is_active:
            entry.resumed_at = datetime.now(timezone.utc)
        return entry

    def is_paused(self, pipeline_id: str) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active

    def entry_for(self, pipeline_id: str) -> Optional[PauseEntry]:
        return self._entries.get(pipeline_id)

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> PauserResult:
        result = PauserResult()
        for snap in snapshots:
            if self.is_paused(snap.pipeline_id):
                result.paused.append(snap.pipeline_id)
            else:
                result.active.append(snap.pipeline_id)
        return result
