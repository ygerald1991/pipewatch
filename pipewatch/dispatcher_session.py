from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_dispatcher import DispatchRule, DispatcherResult, dispatch_snapshots


class DispatcherSession:
    def __init__(self, default_target: Optional[str] = None) -> None:
        self._snapshots: Dict[str, PipelineSnapshot] = {}
        self._rules: List[DispatchRule] = []
        self._default_target = default_target

    def add_rule(self, rule: DispatchRule) -> None:
        self._rules.append(rule)

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots[snapshot.pipeline_id] = snapshot

    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> DispatcherResult:
        return dispatch_snapshots(
            list(self._snapshots.values()),
            self._rules,
            self._default_target,
        )
