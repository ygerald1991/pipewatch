"""Compare metrics across multiple pipelines to surface relative outliers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ComparisonResult:
    pipeline_id: str
    metric: str
    value: float
    mean: float
    delta: float  # value - mean
    is_outlier: bool

    def __str__(self) -> str:
        direction = "above" if self.delta >= 0 else "below"
        flag = " [OUTLIER]" if self.is_outlier else ""
        return (
            f"{self.pipeline_id} | {self.metric}: {self.value:.4f} "
            f"({direction} mean by {abs(self.delta):.4f}){flag}"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std_dev(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def compare_snapshots(
    snapshots: List[PipelineSnapshot],
    outlier_std_threshold: float = 1.5,
) -> List[ComparisonResult]:
    """Compare error_rate and throughput across snapshots.

    A pipeline is flagged as an outlier if its value deviates from the
    group mean by more than *outlier_std_threshold* standard deviations.
    """
    if not snapshots:
        return []

    metrics_map: Dict[str, Dict[str, float]] = {
        "error_rate": {},
        "throughput": {},
    }

    for snap in snapshots:
        metrics_map["error_rate"][snap.pipeline_id] = snap.error_rate
        metrics_map["throughput"][snap.pipeline_id] = snap.throughput

    results: List[ComparisonResult] = []

    for metric_name, pid_values in metrics_map.items():
        values = list(pid_values.values())
        mean = _mean(values)
        std = _std_dev(values, mean)

        for pid, value in pid_values.items():
            delta = value - mean
            is_outlier = std > 0 and abs(delta) > outlier_std_threshold * std
            results.append(
                ComparisonResult(
                    pipeline_id=pid,
                    metric=metric_name,
                    value=value,
                    mean=mean,
                    delta=delta,
                    is_outlier=is_outlier,
                )
            )

    return results


def outlier_pipelines(results: List[ComparisonResult]) -> List[str]:
    """Return unique pipeline IDs that are outliers on at least one metric."""
    seen = set()
    out = []
    for r in results:
        if r.is_outlier and r.pipeline_id not in seen:
            seen.add(r.pipeline_id)
            out.append(r.pipeline_id)
    return out
