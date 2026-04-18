from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class SilenceEntry:
    pipeline_id: str
    reason: str
    silenced_at: datetime
    duration_seconds: float

    @property
    def expires_at(self) -> datetime:
        return self.silenced_at + timedelta(seconds=self.duration_seconds)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at

    def __str__(self) -> str:
        status = "active" if self.is_active() else "expired"
        return f"SilenceEntry({self.pipeline_id}, {status}, expires={self.expires_at.isoformat()})"


@dataclass
class SilencerResult:
    silenced: List[str] = field(default_factory=list)
    allowed: List[str] = field(default_factory=list)

    @property
    def total_silenced(self) -> int:
        return len(self.silenced)

    @property
    def total_allowed(self) -> int:
        return len(self.allowed)


class PipelineSilencer:
    def __init__(self) -> None:
        self._entries: Dict[str, SilenceEntry] = {}

    def silence(self, pipeline_id: str, reason: str, duration_seconds: float,
                now: Optional[datetime] = None) -> SilenceEntry:
        entry = SilenceEntry(
            pipeline_id=pipeline_id,
            reason=reason,
            silenced_at=now or datetime.utcnow(),
            duration_seconds=duration_seconds,
        )
        self._entries[pipeline_id] = entry
        return entry

    def lift(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def is_silenced(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active(now)

    def filter_snapshots(self, snapshots: List[PipelineSnapshot],
                         now: Optional[datetime] = None) -> SilencerResult:
        result = SilencerResult()
        for snap in snapshots:
            if self.is_silenced(snap.pipeline_id, now):
                result.silenced.append(snap.pipeline_id)
            else:
                result.allowed.append(snap.pipeline_id)
        return result

    def active_entries(self, now: Optional[datetime] = None) -> List[SilenceEntry]:
        return [e for e in self._entries.values() if e.is_active(now)]
