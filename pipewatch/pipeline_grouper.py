"""Group pipelines by shared properties and compute aggregate metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PipelineGroup:
    """A named collection of pipeline snapshots."""

    name: str
    snapshots: List[PipelineSnapshot] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return [s.pipeline_id for s in self.snapshots]

    def avg_error_rate(self) -> Optional[float]:
        rates = [s.error_rate for s in self.snapshots if s.error_rate is not None]
        if not rates:
            return None
        return sum(rates) / len(rates)

    def avg_throughput(self) -> Optional[float]:
        values = [s.throughput for s in self.snapshots if s.throughput is not None]
        if not values:
            return None
        return sum(values) / len(values)

    def size(self) -> int:
        return len(self.snapshots)


GroupKeyFn = Callable[[PipelineSnapshot], Optional[str]]


class PipelineGrouper:
    """Groups pipeline snapshots using a key function."""

    def __init__(self, key_fn: GroupKeyFn) -> None:
        self._key_fn = key_fn
        self._groups: Dict[str, PipelineGroup] = {}

    def add(self, snapshot: PipelineSnapshot) -> None:
        key = self._key_fn(snapshot)
        if key is None:
            return
        if key not in self._groups:
            self._groups[key] = PipelineGroup(name=key)
        self._groups[key].snapshots.append(snapshot)

    def groups(self) -> List[PipelineGroup]:
        return list(self._groups.values())

    def group(self, name: str) -> Optional[PipelineGroup]:
        return self._groups.get(name)

    def group_names(self) -> List[str]:
        return list(self._groups.keys())


def group_by_prefix(snapshots: List[PipelineSnapshot], separator: str = "_") -> PipelineGrouper:
    """Convenience: group snapshots by the first segment of their pipeline_id."""

    def _key(s: PipelineSnapshot) -> Optional[str]:
        parts = s.pipeline_id.split(separator, 1)
        return parts[0] if parts else None

    grouper = PipelineGrouper(key_fn=_key)
    for snapshot in snapshots:
        grouper.add(snapshot)
    return grouper
