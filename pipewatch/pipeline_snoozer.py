from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class SnoozeEntry:
    pipeline_id: str
    snoozed_at: datetime
    duration_seconds: float
    reason: str

    def expires_at(self) -> datetime:
        return self.snoozed_at + timedelta(seconds=self.duration_seconds)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at()

    def __str__(self) -> str:
        status = "active" if self.is_active() else "expired"
        return f"SnoozeEntry({self.pipeline_id}, {status}, expires={self.expires_at().isoformat()})"


@dataclass
class SnoozerResult:
    entries: List[SnoozeEntry] = field(default_factory=list)

    @property
    def total_snoozed(self) -> int:
        return sum(1 for e in self.entries if e.is_active())

    @property
    def total_expired(self) -> int:
        return sum(1 for e in self.entries if not e.is_active())

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineSnoozer:
    def __init__(self) -> None:
        self._entries: Dict[str, SnoozeEntry] = {}

    def snooze(self, pipeline_id: str, duration_seconds: float, reason: str = "") -> SnoozeEntry:
        entry = SnoozeEntry(
            pipeline_id=pipeline_id,
            snoozed_at=datetime.utcnow(),
            duration_seconds=duration_seconds,
            reason=reason,
        )
        self._entries[pipeline_id] = entry
        return entry

    def unsnooze(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def is_snoozed(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active(now)

    def run(self, snapshots: List[PipelineSnapshot]) -> SnoozerResult:
        entries = []
        for snapshot in snapshots:
            pid = snapshot.pipeline_id
            if pid in self._entries:
                entries.append(self._entries[pid])
        return SnoozerResult(entries=entries)

    def all_entries(self) -> SnoozerResult:
        return SnoozerResult(entries=list(self._entries.values()))
