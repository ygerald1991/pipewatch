from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_gatekeeper import GatekeeperResult, PipelineGatekeeper


class GatekeeperSession:
    def __init__(
        self,
        max_error_rate: float = 0.5,
        min_throughput: float = 0.0,
        overrides: Optional[Dict[str, bool]] = None,
    ) -> None:
        self._gatekeeper = PipelineGatekeeper(
            max_error_rate=max_error_rate,
            min_throughput=min_throughput,
            overrides=overrides or {},
        )
        self._snapshots: Dict[str, PipelineSnapshot] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots[snapshot.pipeline_id] = snapshot

    def set_override(self, pipeline_id: str, allowed: bool) -> None:
        self._gatekeeper._overrides[pipeline_id] = allowed

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> Optional[GatekeeperResult]:
        if not self._snapshots:
            return None
        return self._gatekeeper.run(list(self._snapshots.values()))
