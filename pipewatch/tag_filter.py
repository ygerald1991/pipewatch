"""Tag-based filtering for pipeline metrics and reports."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

from pipewatch.metrics import PipelineMetric


@dataclass
class TagFilter:
    """Selects pipelines that match ALL required tags."""

    required: Set[str] = field(default_factory=set)
    excluded: Set[str] = field(default_factory=set)

    def matches(self, tags: Iterable[str]) -> bool:
        """Return True if *tags* satisfies the filter criteria."""
        tag_set = set(tags)
        if self.required and not self.required.issubset(tag_set):
            return False
        if self.excluded and self.excluded & tag_set:
            return False
        return True


# Pipeline-level tag registry: pipeline_id -> set of tags
_PipelineTags = Dict[str, Set[str]]


@dataclass
class TagRegistry:
    """Stores tags associated with each pipeline."""

    _tags: _PipelineTags = field(default_factory=dict)

    def tag(self, pipeline_id: str, *tags: str) -> None:
        """Associate one or more tags with *pipeline_id*."""
        self._tags.setdefault(pipeline_id, set()).update(tags)

    def tags_for(self, pipeline_id: str) -> Set[str]:
        """Return the set of tags for *pipeline_id* (empty if unknown)."""
        return set(self._tags.get(pipeline_id, set()))

    def pipeline_ids(self) -> List[str]:
        """Return all registered pipeline IDs."""
        return list(self._tags.keys())

    def filter_pipelines(
        self, tag_filter: TagFilter, candidates: Optional[List[str]] = None
    ) -> List[str]:
        """Return pipeline IDs from *candidates* that match *tag_filter*.

        If *candidates* is None, all registered pipelines are considered.
        """
        pool = candidates if candidates is not None else self.pipeline_ids()
        return [
            pid for pid in pool if tag_filter.matches(self.tags_for(pid))
        ]


def filter_metrics(
    metrics: List[PipelineMetric],
    registry: TagRegistry,
    tag_filter: TagFilter,
) -> List[PipelineMetric]:
    """Return only the metrics whose pipeline matches *tag_filter*."""
    allowed = set(registry.filter_pipelines(tag_filter))
    return [m for m in metrics if m.pipeline_id in allowed]
