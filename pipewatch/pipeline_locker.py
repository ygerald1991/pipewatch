from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class LockEntry:
    pipeline_id: str
    locked_by: str
    reason: str
    locked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __str__(self) -> str:
        return f"LockEntry({self.pipeline_id}, locked_by={self.locked_by}, reason={self.reason})"


@dataclass
class LockerResult:
    entries: List[LockEntry]
    snapshots: List[PipelineSnapshot]

    @property
    def total_locked(self) -> int:
        return len(self.entries)

    @property
    def total_unlocked(self) -> int:
        return len(self.snapshots) - self.total_locked

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineLocker:
    def __init__(self) -> None:
        self._locks: Dict[str, LockEntry] = {}

    def lock(self, pipeline_id: str, locked_by: str, reason: str = "") -> LockEntry:
        entry = LockEntry(pipeline_id=pipeline_id, locked_by=locked_by, reason=reason)
        self._locks[pipeline_id] = entry
        return entry

    def unlock(self, pipeline_id: str) -> bool:
        if pipeline_id in self._locks:
            del self._locks[pipeline_id]
            return True
        return False

    def is_locked(self, pipeline_id: str) -> bool:
        return pipeline_id in self._locks

    def lock_entry(self, pipeline_id: str) -> Optional[LockEntry]:
        return self._locks.get(pipeline_id)

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> LockerResult:
        locked = [self._locks[s.pipeline_id] for s in snapshots if s.pipeline_id in self._locks]
        return LockerResult(entries=locked, snapshots=snapshots)

    @property
    def locked_pipeline_ids(self) -> List[str]:
        return list(self._locks.keys())
