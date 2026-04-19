from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CloneResult:
    source_id: str
    clone_id: str
    snapshot_count: int
    snapshots: List[PipelineSnapshot] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"CloneResult(source={self.source_id}, clone={self.clone_id}, "
            f"snapshots={self.snapshot_count})"
        )


@dataclass
class ClonerResult:
    clones: List[CloneResult] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return [c.clone_id for c in self.clones]

    def for_clone(self, clone_id: str) -> Optional[CloneResult]:
        for c in self.clones:
            if c.clone_id == clone_id:
                return c
        return None

    def total_cloned(self) -> int:
        return sum(c.snapshot_count for c in self.clones)


def clone_snapshots(
    snapshots_by_pipeline: dict[str, List[PipelineSnapshot]],
    id_map: dict[str, str],
) -> ClonerResult:
    """Clone snapshot lists under new pipeline IDs according to id_map.

    Args:
        snapshots_by_pipeline: mapping of original pipeline_id -> snapshots.
        id_map: mapping of original pipeline_id -> new clone pipeline_id.

    Returns:
        ClonerResult containing one CloneResult per mapped pipeline.
    """
    results: List[CloneResult] = []
    for source_id, clone_id in id_map.items():
        originals = snapshots_by_pipeline.get(source_id, [])
        cloned: List[PipelineSnapshot] = []
        for snap in originals:
            cloned.append(
                PipelineSnapshot(
                    pipeline_id=clone_id,
                    metrics=list(snap.metrics),
                    timestamp=snap.timestamp,
                )
            )
        results.append(
            CloneResult(
                source_id=source_id,
                clone_id=clone_id,
                snapshot_count=len(cloned),
                snapshots=cloned,
            )
        )
    return ClonerResult(clones=results)
