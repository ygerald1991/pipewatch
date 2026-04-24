from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class InhibitEntry:
    pipeline_id: str
    reason: str
    inhibited_at: datetime
    inhibited_by: str

    def __str__(self) -> str:
        return (
            f"InhibitEntry(pipeline={self.pipeline_id}, "
            f"reason={self.reason!r}, by={self.inhibited_by})"
        )


@dataclass
class InhibitorResult:
    entries: List[InhibitEntry] = field(default_factory=list)

    @property
    def total_inhibited(self) -> int:
        return sum(1 for e in self.entries if e.pipeline_id in self._inhibited_ids)

    @property
    def total_allowed(self) -> int:
        return len(self.entries) - self.total_inhibited

    @property
    def _inhibited_ids(self) -> set:
        return {e.pipeline_id for e in self.entries}

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineInhibitor:
    def __init__(self) -> None:
        self._inhibited: Dict[str, InhibitEntry] = {}

    def inhibit(self, pipeline_id: str, reason: str, inhibited_by: str = "system") -> InhibitEntry:
        entry = InhibitEntry(
            pipeline_id=pipeline_id,
            reason=reason,
            inhibited_at=datetime.utcnow(),
            inhibited_by=inhibited_by,
        )
        self._inhibited[pipeline_id] = entry
        return entry

    def release(self, pipeline_id: str) -> bool:
        if pipeline_id in self._inhibited:
            del self._inhibited[pipeline_id]
            return True
        return False

    def is_inhibited(self, pipeline_id: str) -> bool:
        return pipeline_id in self._inhibited

    def entry_for(self, pipeline_id: str) -> Optional[InhibitEntry]:
        return self._inhibited.get(pipeline_id)

    @property
    def inhibited_pipeline_ids(self) -> List[str]:
        return list(self._inhibited.keys())

    def evaluate(self, snapshots: List[PipelineSnapshot]) -> InhibitorResult:
        entries = []
        for snapshot in snapshots:
            inhibit_entry = self._inhibited.get(snapshot.pipeline_id)
            if inhibit_entry:
                entries.append(inhibit_entry)
        return InhibitorResult(entries=entries)
