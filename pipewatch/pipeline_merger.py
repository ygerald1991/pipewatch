"""Merge multiple pipeline snapshots into a unified view."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class MergeResult:
    pipeline_id: str
    snapshot_count: int
    avg_error_rate: Optional[float]
    avg_throughput: Optional[float]
    max_error_rate: Optional[float]
    min_throughput: Optional[float]
    source_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        er = f"{self.avg_error_rate:.3f}" if self.avg_error_rate is not None else "n/a"
        tp = f"{self.avg_throughput:.1f}" if self.avg_throughput is not None else "n/a"
        return (
            f"MergeResult(pipeline={self.pipeline_id}, "
            f"snapshots={self.snapshot_count}, "
            f"avg_error_rate={er}, avg_throughput={tp})"
        )


def _safe_avg(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _safe_max(values: List[float]) -> Optional[float]:
    return max(values) if values else None


def _safe_min(values: List[float]) -> Optional[float]:
    return min(values) if values else None


def merge_snapshots(snapshots: List[PipelineSnapshot]) -> List[MergeResult]:
    """Merge a list of snapshots, grouping by pipeline_id."""
    grouped: Dict[str, List[PipelineSnapshot]] = {}
    for snap in snapshots:
        grouped.setdefault(snap.pipeline_id, []).append(snap)

    results: List[MergeResult] = []
    for pipeline_id, group in grouped.items():
        error_rates = [
            s.error_rate for s in group if s.error_rate is not None
        ]
        throughputs = [
            s.throughput for s in group if s.throughput is not None
        ]
        source_ids = [s.pipeline_id for s in group]

        results.append(
            MergeResult(
                pipeline_id=pipeline_id,
                snapshot_count=len(group),
                avg_error_rate=_safe_avg(error_rates),
                avg_throughput=_safe_avg(throughputs),
                max_error_rate=_safe_max(error_rates),
                min_throughput=_safe_min(throughputs),
                source_ids=source_ids,
            )
        )

    return sorted(results, key=lambda r: r.pipeline_id)
