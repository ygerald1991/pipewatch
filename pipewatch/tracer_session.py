from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_tracer import TraceResult, trace_pipeline


class TracerSession:
    def __init__(self) -> None:
        self._snapshots: Dict[str, List[PipelineSnapshot]] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        self._snapshots.setdefault(pid, []).append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> List[TraceResult]:
        results = []
        for pid, snaps in self._snapshots.items():
            sorted_snaps = sorted(snaps, key=lambda s: s.captured_at)
            result = trace_pipeline(pid, sorted_snaps)
            if result is not None:
                results.append(result)
        return results

    def result_for(self, pipeline_id: str) -> Optional[TraceResult]:
        snaps = self._snapshots.get(pipeline_id, [])
        if not snaps:
            return None
        sorted_snaps = sorted(snaps, key=lambda s: s.captured_at)
        return trace_pipeline(pipeline_id, sorted_snaps)
