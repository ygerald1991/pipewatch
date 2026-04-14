"""Tracks pipeline dependencies and detects upstream failures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.metrics import PipelineStatus
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies."""

    _edges: Dict[str, Set[str]] = field(default_factory=dict)  # pipeline -> set of upstreams

    def add_dependency(self, pipeline_id: str, depends_on: str) -> None:
        """Register that *pipeline_id* depends on *depends_on*."""
        self._edges.setdefault(pipeline_id, set()).add(depends_on)
        # Ensure the upstream node is present even if it has no deps itself.
        self._edges.setdefault(depends_on, set())

    def upstreams(self, pipeline_id: str) -> List[str]:
        """Return direct upstream dependencies of *pipeline_id*."""
        return list(self._edges.get(pipeline_id, set()))

    def pipeline_ids(self) -> List[str]:
        """Return all known pipeline IDs in the graph."""
        return list(self._edges.keys())

    def has_pipeline(self, pipeline_id: str) -> bool:
        return pipeline_id in self._edges


@dataclass
class UpstreamImpact:
    pipeline_id: str
    affected_upstreams: List[str]
    degraded_upstreams: List[str]

    @property
    def has_degraded_upstream(self) -> bool:
        return len(self.degraded_upstreams) > 0

    def __str__(self) -> str:
        if not self.has_degraded_upstream:
            return f"{self.pipeline_id}: no degraded upstreams"
        degraded = ", ".join(self.degraded_upstreams)
        return f"{self.pipeline_id}: degraded upstreams -> [{degraded}]"


def analyze_upstream_impact(
    pipeline_id: str,
    graph: DependencyGraph,
    snapshots: Dict[str, PipelineSnapshot],
) -> Optional[UpstreamImpact]:
    """Check whether any upstream pipelines are degraded.

    Returns *None* if *pipeline_id* is not registered in the graph.
    """
    if not graph.has_pipeline(pipeline_id):
        return None

    upstreams = graph.upstreams(pipeline_id)
    degraded: List[str] = []

    for upstream_id in upstreams:
        snapshot = snapshots.get(upstream_id)
        if snapshot is not None and snapshot.status in (
            PipelineStatus.WARNING,
            PipelineStatus.CRITICAL,
        ):
            degraded.append(upstream_id)

    return UpstreamImpact(
        pipeline_id=pipeline_id,
        affected_upstreams=upstreams,
        degraded_upstreams=degraded,
    )
