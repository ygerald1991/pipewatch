from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DeduplicationResult:
    pipeline_id: str
    original_count: int
    deduplicated_count: int
    duplicates_removed: int
    representative: Optional[PipelineSnapshot]

    def __str__(self) -> str:
        return (
            f"{self.pipeline_id}: {self.original_count} snapshots -> "
            f"{self.deduplicated_count} unique ({self.duplicates_removed} removed)"
        )


def _snapshot_key(snapshot: PipelineSnapshot) -> tuple:
    return (
        round(snapshot.error_rate or 0.0, 6),
        round(snapshot.throughput or 0.0, 6),
        snapshot.metric_count,
    )


def deduplicate_snapshots(
    pipeline_id: str, snapshots: List[PipelineSnapshot]
) -> DeduplicationResult:
    if not snapshots:
        return DeduplicationResult(
            pipeline_id=pipeline_id,
            original_count=0,
            deduplicated_count=0,
            duplicates_removed=0,
            representative=None,
        )

    seen = set()
    unique: List[PipelineSnapshot] = []
    for snap in snapshots:
        k = _snapshot_key(snap)
        if k not in seen:
            seen.add(k)
            unique.append(snap)

    return DeduplicationResult(
        pipeline_id=pipeline_id,
        original_count=len(snapshots),
        deduplicated_count=len(unique),
        duplicates_removed=len(snapshots) - len(unique),
        representative=unique[0] if unique else None,
    )


def deduplicate_all(
    groups: dict,
) -> List[DeduplicationResult]:
    results = []
    for pipeline_id, snapshots in groups.items():
        results.append(deduplicate_snapshots(pipeline_id, snapshots))
    return results
