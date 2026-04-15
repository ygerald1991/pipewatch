from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class ScheduleEntry:
    pipeline_id: str
    interval_seconds: int
    last_run: Optional[datetime] = None
    enabled: bool = True

    def is_due(self, now: Optional[datetime] = None) -> bool:
        if not self.enabled:
            return False
        if self.last_run is None:
            return True
        now = now or datetime.utcnow()
        return (now - self.last_run) >= timedelta(seconds=self.interval_seconds)

    def mark_run(self, now: Optional[datetime] = None) -> None:
        self.last_run = now or datetime.utcnow()

    def __str__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        last = self.last_run.isoformat() if self.last_run else "never"
        return (
            f"ScheduleEntry({self.pipeline_id}, "
            f"interval={self.interval_seconds}s, "
            f"last_run={last}, {status})"
        )


@dataclass
class SchedulerResult:
    triggered: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.triggered) + len(self.skipped)


class PipelineScheduler:
    def __init__(self) -> None:
        self._entries: Dict[str, ScheduleEntry] = {}

    def register(self, pipeline_id: str, interval_seconds: int) -> None:
        if pipeline_id not in self._entries:
            self._entries[pipeline_id] = ScheduleEntry(
                pipeline_id=pipeline_id,
                interval_seconds=interval_seconds,
            )

    def disable(self, pipeline_id: str) -> None:
        if pipeline_id in self._entries:
            self._entries[pipeline_id].enabled = False

    def enable(self, pipeline_id: str) -> None:
        if pipeline_id in self._entries:
            self._entries[pipeline_id].enabled = True

    def due_pipelines(self, now: Optional[datetime] = None) -> List[str]:
        now = now or datetime.utcnow()
        return [
            pid
            for pid, entry in self._entries.items()
            if entry.is_due(now)
        ]

    def tick(self, now: Optional[datetime] = None) -> SchedulerResult:
        now = now or datetime.utcnow()
        result = SchedulerResult()
        for pid, entry in self._entries.items():
            if entry.is_due(now):
                entry.mark_run(now)
                result.triggered.append(pid)
            else:
                result.skipped.append(pid)
        return result

    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())

    def entry_for(self, pipeline_id: str) -> Optional[ScheduleEntry]:
        return self._entries.get(pipeline_id)
