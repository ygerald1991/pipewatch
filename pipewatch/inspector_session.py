from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_inspector import InspectionResult, inspect_pipeline


class InspectorSession:
    def __init__(
        self,
        error_rate_warn: float = 0.05,
        throughput_warn: float = 10.0,
    ):
        self._error_rate_warn = error_rate_warn
        self._throughput_warn = throughput_warn
        self._snapshots: Dict[str, List[PipelineSnapshot]] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        self._snapshots.setdefault(pid, []).append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> List[InspectionResult]:
        results = []
        for pid, snaps in self._snapshots.items():
            result = inspect_pipeline(
                pid,
                snaps,
                error_rate_warn=self._error_rate_warn,
                throughput_warn=self._throughput_warn,
            )
            if result is not None:
                results.append(result)
        return results

    def flagged(self) -> List[InspectionResult]:
        return [r for r in self.run() if r.flags]
