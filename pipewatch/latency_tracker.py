"""Tracks and evaluates pipeline processing latency metrics."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric


@dataclass
class LatencyStats:
    pipeline_id: str
    sample_count: int
    mean_latency: float
    min_latency: float
    max_latency: float
    p95_latency: float
    exceeds_threshold: bool
    threshold: float

    def __str__(self) -> str:
        status = "SLOW" if self.exceeds_threshold else "OK"
        return (
            f"[{status}] {self.pipeline_id}: mean={self.mean_latency:.2f}ms "
            f"p95={self.p95_latency:.2f}ms threshold={self.threshold:.2f}ms"
        )


def _percentile(values: List[float], pct: float) -> float:
    """Compute a percentile from a sorted or unsorted list."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    index = (pct / 100.0) * (len(sorted_vals) - 1)
    lower = int(index)
    upper = min(lower + 1, len(sorted_vals) - 1)
    fraction = index - lower
    return sorted_vals[lower] + fraction * (sorted_vals[upper] - sorted_vals[lower])


def compute_latency_stats(
    pipeline_id: str,
    metrics: List[PipelineMetric],
    threshold_ms: float = 500.0,
) -> Optional[LatencyStats]:
    """Compute latency statistics from a list of PipelineMetric records.

    Uses the ``latency_ms`` field on each metric.  Returns ``None`` when
    there are no metrics or none carry latency data.
    """
    samples = [
        m.latency_ms
        for m in metrics
        if m.latency_ms is not None and m.latency_ms >= 0
    ]
    if not samples:
        return None

    mean_val = sum(samples) / len(samples)
    p95_val = _percentile(samples, 95)

    return LatencyStats(
        pipeline_id=pipeline_id,
        sample_count=len(samples),
        mean_latency=mean_val,
        min_latency=min(samples),
        max_latency=max(samples),
        p95_latency=p95_val,
        exceeds_threshold=p95_val > threshold_ms,
        threshold=threshold_ms,
    )
