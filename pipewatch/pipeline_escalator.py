from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class EscalateEntry:
    pipeline_id: str
    level: int
    reason: str
    escalated_at: datetime
    expires_at: Optional[datetime] = None

    def __str__(self) -> str:
        exp = self.expires_at.isoformat() if self.expires_at else "never"
        return (
            f"EscalateEntry(pipeline={self.pipeline_id}, level={self.level}, "
            f"reason={self.reason!r}, expires={exp})"
        )

    def is_active(self, now: Optional[datetime] = None) -> bool:
        if self.expires_at is None:
            return True
        return (now or datetime.utcnow()) < self.expires_at


@dataclass
class EscalatorResult:
    entries: List[EscalateEntry] = field(default_factory=list)

    @property
    def total_escalated(self) -> int:
        return sum(1 for e in self.entries if e.level > 0)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[EscalateEntry]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None

    def at_level(self, level: int) -> List[EscalateEntry]:
        return [e for e in self.entries if e.level == level]


class PipelineEscalator:
    def __init__(self, max_level: int = 3, level_window_seconds: int = 300) -> None:
        self._max_level = max_level
        self._level_window = level_window_seconds
        self._entries: Dict[str, EscalateEntry] = {}

    def escalate(
        self,
        pipeline_id: str,
        reason: str = "",
        now: Optional[datetime] = None,
    ) -> EscalateEntry:
        ts = now or datetime.utcnow()
        current = self._entries.get(pipeline_id)
        current_level = current.level if current else 0
        new_level = min(current_level + 1, self._max_level)
        expires = ts + timedelta(seconds=self._level_window)
        entry = EscalateEntry(
            pipeline_id=pipeline_id,
            level=new_level,
            reason=reason,
            escalated_at=ts,
            expires_at=expires,
        )
        self._entries[pipeline_id] = entry
        return entry

    def reset(self, pipeline_id: str) -> None:
        self._entries.pop(pipeline_id, None)

    def current_level(self, pipeline_id: str) -> int:
        entry = self._entries.get(pipeline_id)
        if entry is None or not entry.is_active():
            return 0
        return entry.level

    def run(self, snapshots: List[PipelineSnapshot]) -> EscalatorResult:
        entries = []
        for snap in snapshots:
            entry = self._entries.get(snap.pipeline_id)
            if entry and entry.is_active():
                entries.append(entry)
            else:
                entries.append(
                    EscalateEntry(
                        pipeline_id=snap.pipeline_id,
                        level=0,
                        reason="",
                        escalated_at=datetime.utcnow(),
                        expires_at=None,
                    )
                )
        return EscalatorResult(entries=entries)
