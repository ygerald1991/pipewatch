from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class WatchEntry:
    pipeline_id: str
    enabled: bool = True
    notes: str = ""

    def __str__(self) -> str:
        status = "watching" if self.enabled else "paused"
        return f"WatchEntry({self.pipeline_id}, {status})"


@dataclass
class WatcherResult:
    entries: List[WatchEntry] = field(default_factory=list)

    @property
    def total_watching(self) -> int:
        return sum(1 for e in self.entries if e.enabled)

    @property
    def total_paused(self) -> int:
        return sum(1 for e in self.entries if not e.enabled)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineWatcher:
    def __init__(self) -> None:
        self._entries: Dict[str, WatchEntry] = {}

    def watch(self, pipeline_id: str, notes: str = "") -> WatchEntry:
        entry = WatchEntry(pipeline_id=pipeline_id, enabled=True, notes=notes)
        self._entries[pipeline_id] = entry
        return entry

    def unwatch(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            del self._entries[pipeline_id]
            return True
        return False

    def pause(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            self._entries[pipeline_id].enabled = False
            return True
        return False

    def resume(self, pipeline_id: str) -> bool:
        if pipeline_id in self._entries:
            self._entries[pipeline_id].enabled = True
            return True
        return False

    def is_watching(self, pipeline_id: str) -> bool:
        entry = self._entries.get(pipeline_id)
        return entry is not None and entry.enabled

    def result(self) -> WatcherResult:
        return WatcherResult(entries=list(self._entries.values()))

    def filter_snapshots(self, snapshots: List[PipelineSnapshot]) -> List[PipelineSnapshot]:
        return [s for s in snapshots if self.is_watching(s.pipeline_id)]
