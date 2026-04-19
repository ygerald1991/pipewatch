from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CooldownEntry:
    pipeline_id: str
    started_at: datetime
    duration_seconds: float
    reason: str

    def expires_at(self) -> datetime:
        return self.started_at + timedelta(seconds=self.duration_seconds)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at()

    def __str__(self) -> str:
        return (
            f"CooldownEntry(pipeline={self.pipeline_id}, "
            f"expires={self.expires_at().isoformat()}, reason={self.reason!r})"
        )


@dataclass
class CooldownResult:
    entries: List[CooldownEntry] = field(default_factory=list)
    snapshots: List[PipelineSnapshot] = field(default_factory=list)

    @property
    def total_cooling(self) -> int:
        return sum(1 for e in self.entries if e.is_active())

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineCooldown:
    def __init__(self) -> None:
        self._entries: Dict[str, CooldownEntry] = {}

    def cool(self, pipeline_id: str, duration_seconds: float, reason: str = "") -> CooldownEntry:
        entry = CooldownEntry(
            pipeline_id=pipeline_id,
            started_at=datetime.utcnow(),
            duration_seconds=duration_seconds,
            reason=reason,
        )
        self._entries[pipeline_id] = entry
        return entry

    def release(self, pipeline_id: str) -> None:
        self._entries.pop(pipeline_id, None)

    def is_cooling(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active(now)

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> CooldownResult:
        active = [e for e in self._entries.values() if e.is_active()]
        allowed = [s for s in snapshots if not self.is_cooling(s.pipeline_id)]
        return CooldownResult(entries=active, snapshots=allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())
