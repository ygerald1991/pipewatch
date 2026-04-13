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
    if abs(slope) <= stable_threshold:
        direction = "stable"
    elif slope > 0:
        direction = "degrading"
    else:
        direction = "improving"
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
    if abs(slope) <= stable_threshold:
        direction = "stable"
    elif slope > 0:
        direction = "improving"
    else:
        direction = "degrading"
    return TrendResult(
        pipeline_id=pipeline_id,
        metric_name="throughput",
        direction=direction,
        delta=delta,
        sample_count=len(metrics),
    )
