from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_scorer import score_pipelines, ScoringResult


class ScorerSession:
    def __init__(self) -> None:
        self._snapshots: List[PipelineSnapshot] = []

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return [s.pipeline_id for s in self._snapshots]

    def run(self) -> Optional[ScoringResult]:
        if not self._snapshots:
            return None
        return score_pipelines(self._snapshots)

    def top(self, n: int = 3) -> List:
        result = self.run()
        if result is None:
            return []
        return result.top(n)

    def bottom(self, n: int = 3) -> List:
        result = self.run()
        if result is None:
            return []
        return result.bottom(n)
