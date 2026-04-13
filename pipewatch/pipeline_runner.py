"""Orchestrates metric collection, threshold evaluation, and alert dispatch."""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.collector import MetricsCollector
from pipewatch.alert_manager import AlertManager
from pipewatch.reporter import generate_report, PipelineReport
from pipewatch.threshold_evaluator import evaluate_metric_thresholds
from pipewatch.thresholds import ThresholdConfig
from pipewatch.metrics import PipelineMetric


@dataclass
class RunResult:
    pipeline_id: str
    report: PipelineReport
    threshold_violations: List[str] = field(default_factory=list)
    alerts_fired: int = 0

    @property
    def healthy(self) -> bool:
        return not self.threshold_violations and self.alerts_fired == 0


def run_pipeline_check(
    pipeline_id: str,
    collector: MetricsCollector,
    alert_manager: AlertManager,
    threshold_config: Optional[ThresholdConfig] = None,
) -> RunResult:
    """Run a full health check cycle for a single pipeline."""
    metrics: List[PipelineMetric] = collector.history(pipeline_id)

    report = generate_report(pipeline_id, metrics)

    violations: List[str] = []
    if threshold_config and metrics:
        latest = collector.latest(pipeline_id)
        if latest is not None:
            violations = evaluate_metric_thresholds(latest, threshold_config)

    fired_alerts = alert_manager.check(pipeline_id, collector)

    return RunResult(
        pipeline_id=pipeline_id,
        report=report,
        threshold_violations=violations,
        alerts_fired=len(fired_alerts),
    )
