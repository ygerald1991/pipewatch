from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ActivationEntry:
    pipeline_id: str
    activated_at: datetime
    activated_by: str

    def __str__(self) -> str:
        return f"ActivationEntry({self.pipeline_id}, by={self.activated_by}, at={self.activated_at.isoformat()})"


@dataclass
class ActivatorResult:
    entries: List[ActivationEntry] = field(default_factory=list)

    @property
    def total_active(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def is_active(self, pipeline_id: str) -> bool:
        return any(e.pipeline_id == pipeline_id for e in self.entries)


class PipelineActivator:
    def __init__(self) -> None:
        self._active: Dict[str, ActivationEntry] = {}

    def activate(self, pipeline_id: str, activated_by: str = "system") -> ActivationEntry:
        entry = ActivationEntry(
            pipeline_id=pipeline_id,
            activated_at=datetime.utcnow(),
            activated_by=activated_by,
        )
        self._active[pipeline_id] = entry
        return entry

    def deactivate(self, pipeline_id: str) -> None:
        self._active.pop(pipeline_id, None)

    def is_active(self, pipeline_id: str) -> bool:
        return pipeline_id in self._active

    def entry_for(self, pipeline_id: str) -> Optional[ActivationEntry]:
        return self._active.get(pipeline_id)

    def run(self, snapshots: List[PipelineSnapshot]) -> ActivatorResult:
        entries = [
            self._active[s.pipeline_id]
            for s in snapshots
            if s.pipeline_id in self._active
        ]
        return ActivatorResult(entries=entries)
