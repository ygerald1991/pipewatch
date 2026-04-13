"""Pipeline health report generation for pipewatch."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus, evaluate_status, error_rate, throughput
from pipewatch.alerts import Alert


@dataclass
class PipelineReport:
    """A snapshot report of a pipeline's health at a point in time."""

    pipeline_id: str
    generated_at: datetime
    status: PipelineStatus
    error_rate: float
    throughput: float
    total_processed: int
    total_failed: int
    active_alerts: List[Alert] = field(default_factory=list)

    def summary(self) -> str:
        """Return a human-readable summary of the report."""
        alert_count = len(self.active_alerts)
        lines = [
            f"Pipeline Report: {self.pipeline_id}",
            f"  Generated At : {self.generated_at.isoformat()}",
            f"  Status       : {self.status.value}",
            f"  Error Rate   : {self.error_rate:.2%}",
            f"  Throughput   : {self.throughput:.2f} records/sec",
            f"  Processed    : {self.total_processed}",
            f"  Failed       : {self.total_failed}",
            f"  Active Alerts: {alert_count}",
        ]
        if self.active_alerts:
            for alert in self.active_alerts:
                lines.append(f"    - {alert}")
        return "\n".join(lines)


def generate_report(
    pipeline_id: str,
    metric: PipelineMetric,
    active_alerts: Optional[List[Alert]] = None,
    error_threshold: float = 0.05,
    warn_threshold: float = 0.02,
) -> PipelineReport:
    """Generate a PipelineReport from a single PipelineMetric snapshot."""
    er = error_rate(metric)
    tp = throughput(metric)
    status = evaluate_status(metric, error_threshold=error_threshold, warn_threshold=warn_threshold)

    return PipelineReport(
        pipeline_id=pipeline_id,
        generated_at=datetime.utcnow(),
        status=status,
        error_rate=er,
        throughput=tp,
        total_processed=metric.records_processed,
        total_failed=metric.records_failed,
        active_alerts=active_alerts or [],
    )
