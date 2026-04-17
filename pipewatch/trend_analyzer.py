"""Trend analysis for pipeline metrics over time."""

from dataclasses import dataclass
from typing import List, Optional
from pipewatch.metrics import PipelineMetric, error_rate, throughput


@dataclass
class TrendResult:
    pipeline_id: str
    metric_name: str
    direction: str  # 'improving', 'degrading', 'stable'
    delta: float
    sample_count: int

    def __str__(self) -> str:
        return (
            f"[{self.pipeline_id}] {self.metric_name}: {self.direction} "
            f"(delta={self.delta:+.4f}, n={self.sample_count})"
        )


def _linear_slope(values: List[float]) -> float:
    """Return the slope of a least-squares linear fit."""
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator != 0 else 0.0


def _classify_direction(slope: float, threshold: float, higher_is_better: bool) -> str:
    """Classify a slope as 'improving', 'degrading', or 'stable'.

    Args:
        slope: The computed linear slope of the metric values.
        threshold: Absolute slope value below which the trend is considered stable.
        higher_is_better: If True, a positive slope is improving; otherwise degrading.
    """
    if abs(slope) <= threshold:
        return "stable"
    if slope > 0:
        return "improving" if higher_is_better else "degrading"
    return "degrading" if higher_is_better else "improving"


def analyze_error_rate_trend(
    pipeline_id: str,
    metrics: List[PipelineMetric],
    stable_threshold: float = 0.005,
) -> Optional[TrendResult]:
    """Analyze the trend of error rate across a sequence of metrics."""
    if len(metrics) < 2:
        return None
    rates = [error_rate(m) for m in metrics]
    slope = _linear_slope(rates)
    delta = rates[-1] - rates[0]
    direction = _classify_direction(slope, stable_threshold, higher_is_better=False)
    return TrendResult(
        pipeline_id=pipeline_id,
        metric_name="error_rate",
        direction=direction,
        delta=delta,
        sample_count=len(metrics),
    )


def analyze_throughput_trend(
    pipeline_id: str,
    metrics: List[PipelineMetric],
    stable_threshold: float = 0.5,
) -> Optional[TrendResult]:
    """Analyze the trend of throughput across a sequence of metrics."""
    if len(metrics) < 2:
        return None
    rates = [throughput(m) for m in metrics]
    slope = _linear_slope(rates)
    delta = rates[-1] - rates[0]
    direction = _classify_direction(slope, stable_threshold, higher_is_better=True)
    return TrendResult(
        pipeline_id=pipeline_id,
        metric_name="throughput",
        direction=direction,
        delta=delta,
        sample_count=len(metrics),
    )
