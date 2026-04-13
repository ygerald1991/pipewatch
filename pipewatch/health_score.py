"""Computes a composite health score for a pipeline based on its metrics."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetric, error_rate, throughput


@dataclass
class HealthScore:
    pipeline_id: str
    score: float  # 0.0 (critical) to 100.0 (perfect)
    error_rate: float
    throughput: float
    grade: str

    def __str__(self) -> str:
        return f"[{self.pipeline_id}] Health: {self.score:.1f}/100 ({self.grade})"


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    elif score >= 40:
        return "D"
    else:
        return "F"


def compute_health_score(
    metric: PipelineMetric,
    max_throughput: Optional[float] = None,
    error_weight: float = 0.7,
    throughput_weight: float = 0.3,
) -> HealthScore:
    """Compute a weighted health score from error rate and throughput.

    Args:
        metric: The pipeline metric to evaluate.
        max_throughput: Expected maximum throughput for normalization.
            If None, throughput component is skipped and error_weight is 1.0.
        error_weight: Weight for error rate component (0.0 to 1.0).
        throughput_weight: Weight for throughput component (0.0 to 1.0).

    Returns:
        A HealthScore instance with the computed score and grade.
    """
    er = error_rate(metric)
    tp = throughput(metric)

    # Error rate component: 0% error = 100 points, 100% error = 0 points
    error_component = max(0.0, (1.0 - er) * 100.0)

    if max_throughput is None or max_throughput <= 0:
        score = error_component
    else:
        # Throughput component: normalized to max_throughput
        tp_ratio = min(tp / max_throughput, 1.0)
        throughput_component = tp_ratio * 100.0
        total_weight = error_weight + throughput_weight
        score = (
            error_component * (error_weight / total_weight)
            + throughput_component * (throughput_weight / total_weight)
        )

    score = max(0.0, min(100.0, score))
    return HealthScore(
        pipeline_id=metric.pipeline_id,
        score=round(score, 2),
        error_rate=er,
        throughput=tp,
        grade=_grade(score),
    )
