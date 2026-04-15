"""Pipeline profiler: captures runtime characteristics of pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PipelineProfile:
    pipeline_id: str
    sample_count: int
    avg_error_rate: Optional[float]
    avg_throughput: Optional[float]
    peak_throughput: Optional[float]
    min_throughput: Optional[float]
    error_rate_variance: Optional[float]

    def __str__(self) -> str:
        return (
            f"Profile({self.pipeline_id}: samples={self.sample_count}, "
            f"avg_err={self.avg_error_rate:.4f}, avg_tp={self.avg_throughput:.2f})"
        )


def _safe_mean(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _safe_variance(values: List[float], mean: float) -> Optional[float]:
    if len(values) < 2:
        return None
    return sum((v - mean) ** 2 for v in values) / len(values)


def profile_pipeline(snapshots: List[PipelineSnapshot]) -> Optional[PipelineProfile]:
    """Build a profile from a list of snapshots for a single pipeline."""
    if not snapshots:
        return None

    pipeline_id = snapshots[0].pipeline_id

    error_rates = [
        s.error_rate for s in snapshots if s.error_rate is not None
    ]
    throughputs = [
        s.throughput for s in snapshots if s.throughput is not None
    ]

    avg_err = _safe_mean(error_rates)
    avg_tp = _safe_mean(throughputs)
    peak_tp = max(throughputs) if throughputs else None
    min_tp = min(throughputs) if throughputs else None
    variance = _safe_variance(error_rates, avg_err) if avg_err is not None else None

    return PipelineProfile(
        pipeline_id=pipeline_id,
        sample_count=len(snapshots),
        avg_error_rate=avg_err,
        avg_throughput=avg_tp,
        peak_throughput=peak_tp,
        min_throughput=min_tp,
        error_rate_variance=variance,
    )


def profile_all(
    snapshot_map: Dict[str, List[PipelineSnapshot]]
) -> Dict[str, PipelineProfile]:
    """Build profiles for all pipelines in a snapshot map."""
    result: Dict[str, PipelineProfile] = {}
    for pid, snaps in snapshot_map.items():
        profile = profile_pipeline(snaps)
        if profile is not None:
            result[pid] = profile
    return result
