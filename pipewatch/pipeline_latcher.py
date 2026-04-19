from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class LatchEntry:
    pipeline_id: str
    latched_at: datetime
    reason: str
    latched_by: str = "system"

    def __str__(self) -> str:
        return f"LatchEntry({self.pipeline_id}, reason={self.reason}, at={self.latched_at.isoformat()})"


@dataclass
class LatcherResult:
    entries: List[LatchEntry] = field(default_factory=list)

    @property
    def total_latched(self) -> int:
        return sum(1 for e in self.entries if e is not None)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[LatchEntry]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None


class PipelineLatcher:
    def __init__(self) -> None:
        self._latched: Dict[str, LatchEntry] = {}

    def latch(self, pipeline_id: str, reason: str, latched_by: str = "system") -> LatchEntry:
        entry = LatchEntry(
            pipeline_id=pipeline_id,
            latched_at=datetime.utcnow(),
            reason=reason,
            latched_by=latched_by,
        )
        self._latched[pipeline_id] = entry
        return entry

    def unlatch(self, pipeline_id: str) -> bool:
        if pipeline_id in self._latched:
            del self._latched[pipeline_id]
            return True
        return False

    def is_latched(self, pipeline_id: str) -> bool:
        return pipeline_id in self._latched

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> LatcherResult:
        entries = []
        for snap in snapshots:
            entry = self._latched.get(snap.pipeline_id)
            if entry is not None:
                entries.append(entry)
        return LatcherResult(entries=entries)

    @property
    def latched_pipeline_ids(self) -> List[str]:
        return list(self._latched.keys())
