"""Alert definitions and evaluation for pipeline health monitoring."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Optional
from pipewatch.metrics import PipelineMetric, PipelineStatus


class AlertSeverity(Enum):
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Defines a threshold-based alert rule for a pipeline metric."""

    name: str
    pipeline_id: str
    severity: AlertSeverity
    condition: Callable[[PipelineMetric], bool]
    message_template: str

    def evaluate(self, metric: PipelineMetric) -> Optional["Alert"]:
        """Return an Alert if the condition is met, otherwise None."""
        if self.condition(metric):
            message = self.message_template.format(
                pipeline_id=metric.pipeline_id,
                error_rate=metric.error_rate,
                throughput=metric.throughput,
                status=metric.status.value,
            )
            return Alert(
                rule_name=self.name,
                pipeline_id=metric.pipeline_id,
                severity=self.severity,
                message=message,
                metric=metric,
            )
        return None


@dataclass
class Alert:
    """Represents a triggered alert for a pipeline."""

    rule_name: str
    pipeline_id: str
    severity: AlertSeverity
    message: str
    metric: PipelineMetric

    def __str__(self) -> str:
        return f"[{self.severity.value.upper()}] {self.pipeline_id} — {self.message}"


def high_error_rate_rule(
    pipeline_id: str,
    threshold: float = 0.1,
    severity: AlertSeverity = AlertSeverity.CRITICAL,
) -> AlertRule:
    """Factory for an alert rule that fires when error rate exceeds threshold."""
    return AlertRule(
        name="high_error_rate",
        pipeline_id=pipeline_id,
        severity=severity,
        condition=lambda m: m.error_rate > threshold,
        message_template="Error rate {error_rate:.1%} exceeds threshold.",
    )


def low_throughput_rule(
    pipeline_id: str,
    min_records: int = 100,
    severity: AlertSeverity = AlertSeverity.WARNING,
) -> AlertRule:
    """Factory for an alert rule that fires when throughput falls below minimum."""
    return AlertRule(
        name="low_throughput",
        pipeline_id=pipeline_id,
        severity=severity,
        condition=lambda m: m.throughput < min_records,
        message_template=f"Throughput {{throughput}} records/s is below minimum {min_records}.",
    )


def pipeline_down_rule(pipeline_id: str) -> AlertRule:
    """Factory for an alert rule that fires when pipeline status is FAILED."""
    return AlertRule(
        name="pipeline_down",
        pipeline_id=pipeline_id,
        severity=AlertSeverity.CRITICAL,
        condition=lambda m: m.status == PipelineStatus.FAILED,
        message_template="Pipeline {pipeline_id} is in FAILED state.",
    )
