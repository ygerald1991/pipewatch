"""Cross-pipeline metric correlation analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    metric: str
    coefficient: float  # Pearson r in [-1, 1]
    is_significant: bool

    def __str__(self) -> str:
        sig = "*" if self.is_significant else ""
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b} "
            f"[{self.metric}]: r={self.coefficient:.3f}{sig}"
        )


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    """Return Pearson correlation coefficient for two equal-length sequences."""
    n = len(xs)
    if n < 2:
        return None
    mx, my = _mean(xs), _mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denom_x = sum((x - mx) ** 2 for x in xs) ** 0.5
    denom_y = sum((y - my) ** 2 for y in ys) ** 0.5
    if denom_x == 0 or denom_y == 0:
        return None
    return num / (denom_x * denom_y)


def _extract_series(snapshots: List[PipelineSnapshot], metric: str) -> List[float]:
    """Pull per-snapshot scalar values for a named metric."""
    result = []
    for snap in snapshots:
        val = snap.metrics.get(metric)
        if val is not None:
            result.append(float(val))
    return result


def correlate_pipelines(
    snapshots_a: List[PipelineSnapshot],
    snapshots_b: List[PipelineSnapshot],
    metrics: List[str],
    significance_threshold: float = 0.7,
) -> List[CorrelationResult]:
    """Compute pairwise Pearson correlation for each metric across two pipelines."""
    if not snapshots_a or not snapshots_b:
        return []

    pipeline_a = snapshots_a[0].pipeline_id
    pipeline_b = snapshots_b[0].pipeline_id
    results: List[CorrelationResult] = []

    for metric in metrics:
        xs = _extract_series(snapshots_a, metric)
        ys = _extract_series(snapshots_b, metric)
        length = min(len(xs), len(ys))
        if length < 2:
            continue
        r = _pearson(xs[:length], ys[:length])
        if r is None:
            continue
        results.append(
            CorrelationResult(
                pipeline_a=pipeline_a,
                pipeline_b=pipeline_b,
                metric=metric,
                coefficient=round(r, 6),
                is_significant=abs(r) >= significance_threshold,
            )
        )
    return results
