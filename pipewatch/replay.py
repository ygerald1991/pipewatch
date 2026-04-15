"""Replay module for re-processing historical metrics through the alert pipeline.

Allows replaying a slice of collected history through alert rules and
threshold evaluators, useful for debugging and post-mortem analysis.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import PipelineMetric
from pipewatch.collector import MetricsCollector
from pipewatch.alert_manager import AlertManager
from pipewatch.alerts import Alert
from pipewatch.threshold_evaluator import evaluate_metric_thresholds
from pipewatch.thresholds import ThresholdConfig


@dataclass
class ReplayEvent:
    """A single event produced during a replay run."""

    pipeline_id: str
    metric: PipelineMetric
    alerts: List[Alert] = field(default_factory=list)
    threshold_breaches: List[str] = field(default_factory=list)

    def has_issues(self) -> bool:
        """Return True if any alerts or threshold breaches were found."""
        return bool(self.alerts or self.threshold_breaches)


@dataclass
class ReplayResult:
    """Aggregated result of a full replay session."""

    pipeline_id: str
    events: List[ReplayEvent] = field(default_factory=list)

    @property
    def total_metrics(self) -> int:
        return len(self.events)

    @property
    def issue_count(self) -> int:
        return sum(1 for e in self.events if e.has_issues())

    @property
    def alert_count(self) -> int:
        return sum(len(e.alerts) for e in self.events)

    def __str__(self) -> str:
        return (
            f"ReplayResult(pipeline={self.pipeline_id}, "
            f"metrics={self.total_metrics}, issues={self.issue_count}, "
            f"alerts={self.alert_count})"
        )


def replay_pipeline(
    pipeline_id: str,
    collector: MetricsCollector,
    alert_manager: AlertManager,
    threshold_config: Optional[ThresholdConfig] = None,
    limit: Optional[int] = None,
) -> ReplayResult:
    """Replay historical metrics for a pipeline through alert and threshold checks.

    Args:
        pipeline_id: The pipeline whose history to replay.
        collector: The MetricsCollector holding historical data.
        alert_manager: An AlertManager configured with the desired rules.
        threshold_config: Optional ThresholdConfig for threshold breach detection.
        limit: If set, only replay the most recent ``limit`` metrics.

    Returns:
        A ReplayResult summarising every event encountered during replay.
    """
    metrics: List[PipelineMetric] = collector.history(pipeline_id)
    if limit is not None:
        metrics = metrics[-limit:]

    result = ReplayResult(pipeline_id=pipeline_id)

    for metric in metrics:
        # Re-run alert rules against each individual metric
        fired_alerts: List[Alert] = alert_manager.check(metric)

        # Optionally evaluate threshold breaches
        breaches: List[str] = []
        if threshold_config is not None:
            cfg = threshold_config.for_pipeline(pipeline_id)
            breaches = evaluate_metric_thresholds(metric, cfg)

        event = ReplayEvent(
            pipeline_id=pipeline_id,
            metric=metric,
            alerts=fired_alerts,
            threshold_breaches=breaches,
        )
        result.events.append(event)

    return result


def replay_all(
    pipeline_ids: List[str],
    collector: MetricsCollector,
    alert_manager: AlertManager,
    threshold_config: Optional[ThresholdConfig] = None,
    limit: Optional[int] = None,
) -> List[ReplayResult]:
    """Replay history for multiple pipelines.

    Args:
        pipeline_ids: Pipelines to replay.
        collector: Shared MetricsCollector.
        alert_manager: Shared AlertManager.
        threshold_config: Optional shared ThresholdConfig.
        limit: Per-pipeline history limit passed to :func:`replay_pipeline`.

    Returns:
        A list of ReplayResult objects, one per pipeline.
    """
    return [
        replay_pipeline(
            pid,
            collector,
            alert_manager,
            threshold_config=threshold_config,
            limit=limit,
        )
        for pid in pipeline_ids
    ]
