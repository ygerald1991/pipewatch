from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class BumpEntry:
    pipeline_id: str
    previous_priority: int
    new_priority: int
    reason: str
    bumped_at: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        return (
            f"BumpEntry(pipeline={self.pipeline_id}, "
            f"{self.previous_priority} -> {self.new_priority}, reason={self.reason})"
        )


@dataclass
class BumperResult:
    entries: List[BumpEntry] = field(default_factory=list)

    @property
    def total_bumped(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[BumpEntry]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None


class PipelineBumper:
    def __init__(self, default_priority: int = 5) -> None:
        self._priorities: Dict[str, int] = {}
        self._default_priority = default_priority

    def priority_for(self, pipeline_id: str) -> int:
        return self._priorities.get(pipeline_id, self._default_priority)

    def bump(self, pipeline_id: str, by: int = 1, reason: str = "manual") -> BumpEntry:
        previous = self.priority_for(pipeline_id)
        new_priority = previous + by
        self._priorities[pipeline_id] = new_priority
        return BumpEntry(
            pipeline_id=pipeline_id,
            previous_priority=previous,
            new_priority=new_priority,
            reason=reason,
        )

    def reset(self, pipeline_id: str) -> None:
        self._priorities.pop(pipeline_id, None)

    def run(self, snapshots: List[PipelineSnapshot], threshold: float = 0.1) -> BumperResult:
        entries: List[BumpEntry] = []
        for snap in snapshots:
            if snap.error_rate is not None and snap.error_rate >= threshold:
                entry = self.bump(snap.pipeline_id, by=1, reason="high_error_rate")
                entries.append(entry)
        return BumperResult(entries=entries)
