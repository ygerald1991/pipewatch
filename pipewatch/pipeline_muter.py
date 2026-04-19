from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class MuteEntry:
    pipeline_id: str
    muted_at: datetime
    duration_seconds: float
    reason: str

    def expires_at(self) -> datetime:
        return self.muted_at + timedelta(seconds=self.duration_seconds)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at()

    def __str__(self) -> str:
        return (
            f"MuteEntry(pipeline_id={self.pipeline_id!r}, "
            f"reason={self.reason!r}, expires_at={self.expires_at().isoformat()})"
        )


@dataclass
class MuterResult:
    entries: List[MuteEntry] = field(default_factory=list)

    @property
    def total_muted(self) -> int:
        return sum(1 for e in self.entries if e.is_active())

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def is_muted(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        for e in self.entries:
            if e.pipeline_id == pipeline_id and e.is_active(now):
                return True
        return False


class PipelineMuter:
    def __init__(self) -> None:
        self._entries: Dict[str, MuteEntry] = {}

    def mute(
        self,
        pipeline_id: str,
        duration_seconds: float = 300.0,
        reason: str = "",
        now: Optional[datetime] = None,
    ) -> MuteEntry:
        entry = MuteEntry(
            pipeline_id=pipeline_id,
            muted_at=now or datetime.utcnow(),
            duration_seconds=duration_seconds,
            reason=reason,
        )
        self._entries[pipeline_id] = entry
        return entry

    def unmute(self, pipeline_id: str) -> None:
        self._entries.pop(pipeline_id, None)

    def is_muted(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active(now)

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> MuterResult:
        result = MuterResult()
        for snap in snapshots:
            entry = self._entries.get(snap.pipeline_id)
            if entry is not None:
                result.entries.append(entry)
        return result

    def active_entries(self, now: Optional[datetime] = None) -> List[MuteEntry]:
        return [e for e in self._entries.values() if e.is_active(now)]
