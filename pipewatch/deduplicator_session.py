from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_deduplicator import DeduplicationResult, deduplicate_all


class DeduplicatorSession:
    def __init__(self) -> None:
        self._groups: Dict[str, List[PipelineSnapshot]] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        if pid not in self._groups:
            self._groups[pid] = []
        self._groups[pid].append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return list(self._groups.keys())

    def run(self) -> List[DeduplicationResult]:
        return deduplicate_all(self._groups)

    def total_removed(self) -> int:
        return sum(r.duplicates_removed for r in self.run())
