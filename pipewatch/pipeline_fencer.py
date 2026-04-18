from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class FenceEntry:
    pipeline_id: str
    reason: str
    fenced_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None

    def is_active(self) -> bool:
        if self.expires_at is None:
            return True
        return datetime.now(timezone.utc) < self.expires_at

    def __str__(self) -> str:
        exp = self.expires_at.isoformat() if self.expires_at else "never"
        return f"FenceEntry({self.pipeline_id}, reason={self.reason}, expires={exp})"


@dataclass
class FencerResult:
    fenced: List[str] = field(default_factory=list)
    allowed: List[str] = field(default_factory=list)

    @property
    def total_fenced(self) -> int:
        return len(self.fenced)

    @property
    def total_allowed(self) -> int:
        return len(self.allowed)


class PipelineFencer:
    def __init__(self) -> None:
        self._entries: Dict[str, FenceEntry] = {}

    def fence(self, pipeline_id: str, reason: str, expires_at: Optional[datetime] = None) -> FenceEntry:
        entry = FenceEntry(pipeline_id=pipeline_id, reason=reason, expires_at=expires_at)
        self._entries[pipeline_id] = entry
        return entry

    def unfence(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def is_fenced(self, pipeline_id: str) -> bool:
        entry = self._entries.get(pipeline_id)
        if entry is None:
            return False
        if not entry.is_active():
            del self._entries[pipeline_id]
            return False
        return True

    def fenced_pipelines(self) -> List[str]:
        return [pid for pid, e in list(self._entries.items()) if e.is_active()]

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> FencerResult:
        result = FencerResult()
        for snap in snapshots:
            if self.is_fenced(snap.pipeline_id):
                result.fenced.append(snap.pipeline_id)
            else:
                result.allowed.append(snap.pipeline_id)
        return result
