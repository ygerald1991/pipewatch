"""Evaluate pipeline metrics against configurable thresholds."""

from typing import List

from pipewatch.metrics import PipelineMetric, PipelineStatus, error_rate, throughput
from pipewatch.thresholds import ThresholdConfig, DEFAULT_THRESHOLDS
from pipewatch.alerts import Alert, AlertSeverity


def evaluate_metric_thresholds(
    metric: PipelineMetric,
    config: ThresholdConfig = DEFAULT_THRESHOLDS,
) -> List[Alert]:
    """Return a list of Alerts based on threshold evaluation for a single metric."""
    alerts: List[Alert] = []
    cfg = config.for_pipeline(metric.pipeline_id)

    er = error_rate(metric)
    if er >= cfg.error_rate_critical:
        alerts.append(
            Alert(
                pipeline_id=metric.pipeline_id,
                severity=AlertSeverity.CRITICAL,
                message=f"Error rate {er:.1%} exceeds critical threshold {cfg.error_rate_critical:.1%}",
                metric=metric,
            )
        )
    elif er >= cfg.error_rate_warning:
        alerts.append(
            Alert(
                pipeline_id=metric.pipeline_id,
                severity=AlertSeverity.WARNING,
                message=f"Error rate {er:.1%} exceeds warning threshold {cfg.error_rate_warning:.1%}",
                metric=metric,
            )
        )

    tp = throughput(metric)
    if tp > 0 and tp <= cfg.throughput_critical:
        alerts.append(
            Alert(
                pipeline_id=metric.pipeline_id,
                severity=AlertSeverity.CRITICAL,
                message=f"Throughput {tp:.1f} rec/s below critical threshold {cfg.throughput_critical:.1f}",
                metric=metric,
            )
        )
    elif tp > 0 and tp <= cfg.throughput_warning:
        alerts.append(
            Alert(
                pipeline_id=metric.pipeline_id,
                severity=AlertSeverity.WARNING,
                message=f"Throughput {tp:.1f} rec/s below warning threshold {cfg.throughput_warning:.1f}",
                metric=metric,
            )
        )

    return alerts
