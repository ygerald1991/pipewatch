from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.pipeline_watcher import PipelineWatcher, WatcherResult
from pipewatch.snapshot import PipelineSnapshot


class WatcherSession:
    def __init__(self) -> None:
        self._watcher = PipelineWatcher()
        self._snapshots: Dict[str, List[PipelineSnapshot]] = {}

    def watch(self, pipeline_id: str, notes: str = "") -> None:
        self._watcher.watch(pipeline_id, notes=notes)

    def unwatch(self, pipeline_id: str) -> None:
        self._watcher.unwatch(pipeline_id)
        self._snapshots.pop(pipeline_id, None)

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        self._snapshots.setdefault(pid, []).append(snapshot)

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def active_snapshots(self) -> List[PipelineSnapshot]:
        all_snapshots = [s for snaps in self._snapshots.values() for s in snaps]
        return self._watcher.filter_snapshots(all_snapshots)

    def run(self) -> WatcherResult:
        return self._watcher.result()
