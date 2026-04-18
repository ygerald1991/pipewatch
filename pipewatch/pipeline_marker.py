from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class MarkEntry:
    pipeline_id: str
    label: str
    reason: str
    marked_at: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        return f"{self.pipeline_id} [{self.label}]: {self.reason}"


@dataclass
class MarkerResult:
    entries: List[MarkEntry] = field(default_factory=list)

    @property
    def total_marked(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_label(self, label: str) -> List[MarkEntry]:
        return [e for e in self.entries if e.label == label]


class PipelineMarker:
    def __init__(self) -> None:
        self._marks: Dict[str, MarkEntry] = {}

    def mark(self, snapshot: PipelineSnapshot, label: str, reason: str) -> MarkEntry:
        entry = MarkEntry(
            pipeline_id=snapshot.pipeline_id,
            label=label,
            reason=reason,
        )
        self._marks[snapshot.pipeline_id] = entry
        return entry

    def unmark(self, pipeline_id: str) -> bool:
        if pipeline_id in self._marks:
            del self._marks[pipeline_id]
            return True
        return False

    def is_marked(self, pipeline_id: str) -> bool:
        return pipeline_id in self._marks

    def get_mark(self, pipeline_id: str) -> Optional[MarkEntry]:
        return self._marks.get(pipeline_id)

    def all_marks(self) -> MarkerResult:
        return MarkerResult(entries=list(self._marks.values()))

    @property
    def marked_pipeline_ids(self) -> List[str]:
        return list(self._marks.keys())
