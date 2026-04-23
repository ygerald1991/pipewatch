from __future__ import annotations
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_resolver import ResolverResult, resolve_snapshots


class ResolverSession:
    """Session that collects snapshots and resolves their pipeline IDs."""

    def __init__(self, alias_map: Optional[Dict[str, str]] = None) -> None:
        self._snapshots: List[PipelineSnapshot] = []
        self._alias_map: Dict[str, str] = alias_map or {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def set_alias(self, pipeline_id: str, alias: str) -> None:
        self._alias_map[pipeline_id] = alias

    @property
    def pipeline_ids(self) -> List[str]:
        return [s.pipeline_id for s in self._snapshots]

    def run(self) -> Optional[ResolverResult]:
        if not self._snapshots:
            return None
        return resolve_snapshots(self._snapshots, self._alias_map)
