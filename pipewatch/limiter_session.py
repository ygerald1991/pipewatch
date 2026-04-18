from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_limiter import LimiterResult, limit_snapshots


class LimiterSession:
    def __init__(
        self,
        default_limit: int = 100,
        overrides: Optional[Dict[str, int]] = None,
    ) -> None:
        self._default_limit = default_limit
        self._overrides: Dict[str, int] = overrides or {}
        self._snapshots: List[PipelineSnapshot] = []

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def set_limit(self, pipeline_id: str, limit: int) -> None:
        self._overrides[pipeline_id] = limit

    def pipeline_ids(self) -> List[str]:
        seen = []
        for s in self._snapshots:
            if s.pipeline_id not in seen:
                seen.append(s.pipeline_id)
        return seen

    def run(self) -> LimiterResult:
        return limit_snapshots(
            self._snapshots,
            default_limit=self._default_limit,
            overrides=self._overrides,
        )
