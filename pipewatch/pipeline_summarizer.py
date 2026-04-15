"""Summarizes pipeline health across multiple snapshots into a concise report."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


@dataclass
class PipelineSummary:
    pipeline_id: str
    snapshot_count: int
    avg_error_rate: Optional[float]
    avg_throughput: Optional[float]
    dominant_status: PipelineStatus
    degraded_count: int

    def __str__(self) -> str:
        er = f"{self.avg_error_rate:.2%}" if self.avg_error_rate is not None else "n/a"
        tp = f"{self.avg_throughput:.1f}/s" if self.avg_throughput is not None else "n/a"
        return (
            f"[{self.pipeline_id}] status={self.dominant_status.value} "
            f"error_rate={er} throughput={tp} "
            f"snapshots={self.snapshot_count} degraded={self.degraded_count}"
        )


def _dominant_status(snapshots: List[PipelineSnapshot]) -> PipelineStatus:
    counts = {s: 0 for s in PipelineStatus}
    for snap in snapshots:
        counts[snap.status] = counts.get(snap.status, 0) + 1
    return max(counts, key=lambda s: counts[s])


def _safe_avg(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def summarize_pipeline(snapshots: List[PipelineSnapshot]) -> Optional[PipelineSummary]:
    """Produce a summary for a single pipeline across its snapshot history."""
    if not snapshots:
        return None

    pipeline_id = snapshots[0].pipeline_id
    error_rates = [s.error_rate for s in snapshots if s.error_rate is not None]
    throughputs = [s.throughput for s in snapshots if s.throughput is not None]
    degraded = sum(
        1 for s in snapshots if s.status in (PipelineStatus.WARNING, PipelineStatus.CRITICAL)
    )

    return PipelineSummary(
        pipeline_id=pipeline_id,
        snapshot_count=len(snapshots),
        avg_error_rate=_safe_avg(error_rates),
        avg_throughput=_safe_avg(throughputs),
        dominant_status=_dominant_status(snapshots),
        degraded_count=degraded,
    )


def summarize_all(
    snapshot_groups: dict,
) -> List[PipelineSummary]:
    """Summarize multiple pipelines. snapshot_groups maps pipeline_id -> [PipelineSnapshot]."""
    results = []
    for pipeline_id, snapshots in snapshot_groups.items():
        summary = summarize_pipeline(snapshots)
        if summary is not None:
            results.append(summary)
    results.sort(key=lambda s: s.pipeline_id)
    return results
