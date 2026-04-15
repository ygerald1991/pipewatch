"""Filter pipelines by status, error rate, throughput, or custom predicates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class FilterCriteria:
    """Criteria used to filter pipeline snapshots."""

    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    min_throughput: Optional[float] = None
    max_throughput: Optional[float] = None
    required_tags: List[str] = field(default_factory=list)
    pipeline_ids: List[str] = field(default_factory=list)
    custom: Optional[Callable[[PipelineSnapshot], bool]] = None


def _matches_criteria(snapshot: PipelineSnapshot, criteria: FilterCriteria) -> bool:
    """Return True if the snapshot satisfies all criteria."""
    if criteria.pipeline_ids and snapshot.pipeline_id not in criteria.pipeline_ids:
        return False

    er = snapshot.error_rate
    if criteria.min_error_rate is not None and (er is None or er < criteria.min_error_rate):
        return False
    if criteria.max_error_rate is not None and (er is None or er > criteria.max_error_rate):
        return False

    tp = snapshot.throughput
    if criteria.min_throughput is not None and (tp is None or tp < criteria.min_throughput):
        return False
    if criteria.max_throughput is not None and (tp is None or tp > criteria.max_throughput):
        return False

    if criteria.required_tags:
        snapshot_tags = set(getattr(snapshot, "tags", []) or [])
        if not all(t in snapshot_tags for t in criteria.required_tags):
            return False

    if criteria.custom is not None and not criteria.custom(snapshot):
        return False

    return True


def filter_snapshots(
    snapshots: List[PipelineSnapshot],
    criteria: FilterCriteria,
) -> List[PipelineSnapshot]:
    """Return only those snapshots that satisfy *criteria*."""
    return [s for s in snapshots if _matches_criteria(s, criteria)]


def partition_snapshots(
    snapshots: List[PipelineSnapshot],
    criteria: FilterCriteria,
) -> tuple[List[PipelineSnapshot], List[PipelineSnapshot]]:
    """Split snapshots into (matching, non-matching) lists."""
    matching, rest = [], []
    for s in snapshots:
        (matching if _matches_criteria(s, criteria) else rest).append(s)
    return matching, rest
