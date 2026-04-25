from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DampenEntry:
    pipeline_id: str
    triggered_at: datetime
    duration_seconds: float
    reason: str
    dampened: bool = True

    def expires_at(self) -> datetime:
        return self.triggered_at + timedelta(seconds=self.duration_seconds)

    def is_active(self) -> bool:
        return datetime.utcnow() < self.expires_at()

    def __str__(self) -> str:
        status = "active" if self.is_active() else "expired"
        return f"DampenEntry({self.pipeline_id}, {status}, reason={self.reason!r})"


@dataclass
class DampenerResult:
    entries: List[DampenEntry] = field(default_factory=list)

    @property
    def total_dampened(self) -> int:
        return sum(1 for e in self.entries if e.dampened)

    @property
    def total_active(self) -> int:
        return sum(1 for e in self.entries if e.is_active())

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineDampener:
    def __init__(self, default_duration_seconds: float = 60.0) -> None:
        self._default_duration = default_duration_seconds
        self._entries: Dict[str, DampenEntry] = {}

    def dampen(
        self,
        pipeline_id: str,
        reason: str = "",
        duration_seconds: Optional[float] = None,
    ) -> DampenEntry:
        duration = duration_seconds if duration_seconds is not None else self._default_duration
        entry = DampenEntry(
            pipeline_id=pipeline_id,
            triggered_at=datetime.utcnow(),
            duration_seconds=duration,
            reason=reason,
        )
        self._entries[pipeline_id] = entry
        return entry

    def release(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def is_dampened(self, pipeline_id: str) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active()

    def apply(self, snapshots: List[PipelineSnapshot]) -> DampenerResult:
        results = []
        for snap in snapshots:
            if self.is_dampened(snap.pipeline_id):
                entry = self._entries[snap.pipeline_id]
                results.append(entry)
        return DampenerResult(entries=results)

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())
