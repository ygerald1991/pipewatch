"""Anomaly detection for pipeline metrics using z-score based outlier detection."""

from dataclasses import dataclass
from typing import List, Optional
import math

from pipewatch.metrics import PipelineMetric, error_rate, throughput


@dataclass
class AnomalyResult:
    pipeline_id: str
    metric_name: str
    current_value: float
    mean: float
    std_dev: float
    z_score: float
    is_anomaly: bool

    def __str__(self) -> str:
        flag = "[ANOMALY]" if self.is_anomaly else "[OK]"
        return (
            f"{flag} {self.pipeline_id} / {self.metric_name}: "
            f"value={self.current_value:.4f}, mean={self.mean:.4f}, "
            f"z={self.z_score:.2f}"
        )


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std_dev(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def detect_anomaly(
    pipeline_id: str,
    metric_name: str,
    history: List[float],
    current: float,
    z_threshold: float = 2.5,
) -> Optional[AnomalyResult]:
    """Detect whether *current* is an anomaly relative to *history*.

    Returns None when there is insufficient history (fewer than 3 points).
    """
    if len(history) < 3:
        return None

    mean = _mean(history)
    std = _std_dev(history, mean)

    if std == 0.0:
        z_score = 0.0
    else:
        z_score = abs(current - mean) / std

    return AnomalyResult(
        pipeline_id=pipeline_id,
        metric_name=metric_name,
        current_value=current,
        mean=mean,
        std_dev=std,
        z_score=z_score,
        is_anomaly=z_score >= z_threshold,
    )


def analyze_metrics_for_anomalies(
    pipeline_id: str,
    history: List[PipelineMetric],
    z_threshold: float = 2.5,
) -> List[AnomalyResult]:
    """Run anomaly detection over error_rate and throughput for a pipeline."""
    if len(history) < 4:
        return []

    past, current_metric = history[:-1], history[-1]

    results: List[AnomalyResult] = []

    er_history = [error_rate(m) for m in past]
    er_current = error_rate(current_metric)
    er_result = detect_anomaly(pipeline_id, "error_rate", er_history, er_current, z_threshold)
    if er_result is not None:
        results.append(er_result)

    tp_history = [throughput(m) for m in past]
    tp_current = throughput(current_metric)
    tp_result = detect_anomaly(pipeline_id, "throughput", tp_history, tp_current, z_threshold)
    if tp_result is not None:
        results.append(tp_result)

    return results
