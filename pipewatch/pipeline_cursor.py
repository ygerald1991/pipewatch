from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CursorEntry:
    pipeline_id: str
    position: int
    total: int

    def __str__(self) -> str:
        return f"CursorEntry(pipeline={self.pipeline_id}, pos={self.position}/{self.total})"

    @property
    def has_next(self) -> bool:
        return self.position < self.total - 1

    @property
    def has_prev(self) -> bool:
        return self.position > 0

    @property
    def progress_pct(self) -> float:
        if self.total == 0:
            return 0.0
        return round(self.position / self.total * 100, 1)


@dataclass
class CursorResult:
    entries: List[CursorEntry] = field(default_factory=list)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    @property
    def total_pipelines(self) -> int:
        return len(self.entries)

    def for_pipeline(self, pipeline_id: str) -> Optional[CursorEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None

    def at_end(self) -> List[CursorEntry]:
        return [e for e in self.entries if not e.has_next]


class PipelineCursor:
    def __init__(self) -> None:
        self._positions: Dict[str, int] = {}
        self._snapshots: Dict[str, List[PipelineSnapshot]] = {}

    def register(self, pipeline_id: str, snapshots: List[PipelineSnapshot]) -> None:
        self._snapshots[pipeline_id] = snapshots
        if pipeline_id not in self._positions:
            self._positions[pipeline_id] = 0

    def advance(self, pipeline_id: str, steps: int = 1) -> Optional[CursorEntry]:
        if pipeline_id not in self._snapshots:
            return None
        total = len(self._snapshots[pipeline_id])
        current = self._positions.get(pipeline_id, 0)
        self._positions[pipeline_id] = min(current + steps, max(total - 1, 0))
        return CursorEntry(pipeline_id=pipeline_id, position=self._positions[pipeline_id], total=total)

    def rewind(self, pipeline_id: str, steps: int = 1) -> Optional[CursorEntry]:
        if pipeline_id not in self._snapshots:
            return None
        total = len(self._snapshots[pipeline_id])
        current = self._positions.get(pipeline_id, 0)
        self._positions[pipeline_id] = max(current - steps, 0)
        return CursorEntry(pipeline_id=pipeline_id, position=self._positions[pipeline_id], total=total)

    def reset(self, pipeline_id: str) -> None:
        if pipeline_id in self._positions:
            self._positions[pipeline_id] = 0

    def current_snapshot(self, pipeline_id: str) -> Optional[PipelineSnapshot]:
        snapshots = self._snapshots.get(pipeline_id)
        if not snapshots:
            return None
        pos = self._positions.get(pipeline_id, 0)
        return snapshots[pos] if pos < len(snapshots) else None

    def run(self) -> CursorResult:
        entries = []
        for pid, snapshots in self._snapshots.items():
            pos = self._positions.get(pid, 0)
            entries.append(CursorEntry(pipeline_id=pid, position=pos, total=len(snapshots)))
        return CursorResult(entries=entries)
