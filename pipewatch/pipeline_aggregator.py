"""Aggregate metrics across multiple pipelines into summary statistics."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class AggregateStats:
    """Summary statistics computed across a collection of pipeline snapshots."""

    pipeline_count: int
    total_metrics: int
    avg_error_rate: Optional[float]
    max_error_rate: Optional[float]
    min_error_rate: Optional[float]
    avg_throughput: Optional[float]
    max_throughput: Optional[float]
    min_throughput: Optional[float]
    degraded_count: int
    pipeline_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"AggregateStats(pipelines={self.pipeline_count}, "
            f"avg_error_rate={self.avg_error_rate:.3f}, "
            f"avg_throughput={self.avg_throughput:.1f}, "
            f"degraded={self.degraded_count})"
        )


def _safe_avg(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def aggregate_snapshots(snapshots: List[PipelineSnapshot]) -> AggregateStats:
    """Compute aggregate statistics across all provided snapshots."""
    if not snapshots:
        return AggregateStats(
            pipeline_count=0,
            total_metrics=0,
            avg_error_rate=None,
            max_error_rate=None,
            min_error_rate=None,
            avg_throughput=None,
            max_throughput=None,
            min_throughput=None,
            degraded_count=0,
            pipeline_ids=[],
        )

    error_rates: List[float] = [
        s.error_rate for s in snapshots if s.error_rate is not None
    ]
    throughputs: List[float] = [
        s.throughput for s in snapshots if s.throughput is not None
    ]

    degraded = sum(
        1 for s in snapshots if s.error_rate is not None and s.error_rate > 0.05
    )

    return AggregateStats(
        pipeline_count=len(snapshots),
        total_metrics=sum(s.metric_count for s in snapshots),
        avg_error_rate=_safe_avg(error_rates),
        max_error_rate=max(error_rates) if error_rates else None,
        min_error_rate=min(error_rates) if error_rates else None,
        avg_throughput=_safe_avg(throughputs),
        max_throughput=max(throughputs) if throughputs else None,
        min_throughput=min(throughputs) if throughputs else None,
        degraded_count=degraded,
        pipeline_ids=[s.pipeline_id for s in snapshots],
    )
