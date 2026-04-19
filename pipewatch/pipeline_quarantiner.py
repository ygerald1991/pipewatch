from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class QuarantineEntry:
    pipeline_id: str
    reason: str
    quarantined_at: datetime
    duration_seconds: float

    def expires_at(self) -> datetime:
        return self.quarantined_at + timedelta(seconds=self.duration_seconds)

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at()

    def __str__(self) -> str:
        status = "active" if self.is_active() else "expired"
        return f"QuarantineEntry({self.pipeline_id}, {status}, reason={self.reason!r})"


@dataclass
class QuarantinerResult:
    entries: List[QuarantineEntry]
    allowed: List[str]

    @property
    def total_quarantined(self) -> int:
        return sum(1 for e in self.entries if e.is_active())

    @property
    def total_allowed(self) -> int:
        return len(self.allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineQuarantiner:
    def __init__(self) -> None:
        self._entries: Dict[str, QuarantineEntry] = {}

    def quarantine(
        self,
        pipeline_id: str,
        reason: str,
        duration_seconds: float = 300.0,
        now: Optional[datetime] = None,
    ) -> QuarantineEntry:
        now = now or datetime.utcnow()
        entry = QuarantineEntry(
            pipeline_id=pipeline_id,
            reason=reason,
            quarantined_at=now,
            duration_seconds=duration_seconds,
        )
        self._entries[pipeline_id] = entry
        return entry

    def release(self, pipeline_id: str) -> None:
        self._entries.pop(pipeline_id, None)

    def is_quarantined(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.is_active(now)

    def run(
        self, snapshots: List[PipelineSnapshot], now: Optional[datetime] = None
    ) -> QuarantinerResult:
        now = now or datetime.utcnow()
        active_entries: List[QuarantineEntry] = []
        allowed: List[str] = []
        for snapshot in snapshots:
            pid = snapshot.pipeline_id
            entry = self._entries.get(pid)
            if entry and entry.is_active(now):
                active_entries.append(entry)
            else:
                allowed.append(pid)
        return QuarantinerResult(entries=active_entries, allowed=allowed)
