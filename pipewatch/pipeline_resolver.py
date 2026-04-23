from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ResolveEntry:
    pipeline_id: str
    resolved_id: str
    alias: Optional[str]
    was_remapped: bool

    def __str__(self) -> str:
        tag = f" (alias: {self.alias})" if self.alias else ""
        flag = " [remapped]" if self.was_remapped else ""
        return f"{self.pipeline_id} -> {self.resolved_id}{tag}{flag}"


@dataclass
class ResolverResult:
    entries: List[ResolveEntry] = field(default_factory=list)

    @property
    def total_remapped(self) -> int:
        return sum(1 for e in self.entries if e.was_remapped)

    @property
    def total_unchanged(self) -> int:
        return sum(1 for e in self.entries if not e.was_remapped)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.resolved_id for e in self.entries]


def resolve_snapshots(
    snapshots: List[PipelineSnapshot],
    alias_map: Optional[Dict[str, str]] = None,
) -> ResolverResult:
    """Resolve pipeline IDs using an optional alias map.

    If a snapshot's pipeline_id appears in alias_map, the resolved_id is
    replaced with the mapped value and was_remapped is set to True.
    """
    if alias_map is None:
        alias_map = {}

    entries: List[ResolveEntry] = []
    for snap in snapshots:
        pid = snap.pipeline_id
        if pid in alias_map:
            resolved = alias_map[pid]
            entries.append(
                ResolveEntry(
                    pipeline_id=pid,
                    resolved_id=resolved,
                    alias=resolved,
                    was_remapped=True,
                )
            )
        else:
            entries.append(
                ResolveEntry(
                    pipeline_id=pid,
                    resolved_id=pid,
                    alias=None,
                    was_remapped=False,
                )
            )
    return ResolverResult(entries=entries)
