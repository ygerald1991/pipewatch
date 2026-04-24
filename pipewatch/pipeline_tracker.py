from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class TrackEntry:
    pipeline_id: str
    snapshot: PipelineSnapshot
    tracked_at: datetime
    label: str = ""

    def __str__(self) -> str:
        label_part = f" [{self.label}]" if self.label else ""
        return f"TrackEntry({self.pipeline_id}{label_part} @ {self.tracked_at.isoformat()})"


@dataclass
class TrackerResult:
    entries: List[TrackEntry] = field(default_factory=list)

    @property
    def total_tracked(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return list({e.pipeline_id for e in self.entries})

    def entries_for(self, pipeline_id: str) -> List[TrackEntry]:
        return [e for e in self.entries if e.pipeline_id == pipeline_id]

    def latest_for(self, pipeline_id: str) -> Optional[TrackEntry]:
        matches = self.entries_for(pipeline_id)
        if not matches:
            return None
        return max(matches, key=lambda e: e.tracked_at)


class PipelineTracker:
    def __init__(self) -> None:
        self._entries: Dict[str, List[TrackEntry]] = {}

    def track(self, snapshot: PipelineSnapshot, label: str = "") -> TrackEntry:
        pid = snapshot.pipeline_id
        entry = TrackEntry(
            pipeline_id=pid,
            snapshot=snapshot,
            tracked_at=datetime.utcnow(),
            label=label,
        )
        self._entries.setdefault(pid, []).append(entry)
        return entry

    def untrack(self, pipeline_id: str) -> int:
        removed = len(self._entries.pop(pipeline_id, []))
        return removed

    def latest(self, pipeline_id: str) -> Optional[TrackEntry]:
        entries = self._entries.get(pipeline_id, [])
        if not entries:
            return None
        return entries[-1]

    def history(self, pipeline_id: str) -> List[TrackEntry]:
        return list(self._entries.get(pipeline_id, []))

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())

    def result(self) -> TrackerResult:
        all_entries = [e for entries in self._entries.values() for e in entries]
        return TrackerResult(entries=all_entries)
