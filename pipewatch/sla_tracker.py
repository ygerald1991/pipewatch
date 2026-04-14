"""SLA tracking for pipeline metrics — checks whether pipelines meet
defined success-rate and throughput targets within a rolling window."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.metrics import PipelineMetric, error_rate, throughput


@dataclass
class SLAPolicy:
    """Defines acceptable limits for a pipeline's SLA."""
    max_error_rate: float = 0.05        # 5 %
    min_throughput: float = 1.0         # records / second
    overrides: dict[str, dict] = field(default_factory=dict)

    def for_pipeline(self, pipeline_id: str) -> "SLAPolicy":
        """Return an SLAPolicy with per-pipeline overrides applied."""
        ov = self.overrides.get(pipeline_id, {})
        return SLAPolicy(
            max_error_rate=ov.get("max_error_rate", self.max_error_rate),
            min_throughput=ov.get("min_throughput", self.min_throughput),
            overrides=self.overrides,
        )


@dataclass
class SLAResult:
    pipeline_id: str
    met: bool
    error_rate_ok: bool
    throughput_ok: bool
    actual_error_rate: float
    actual_throughput: float
    policy: SLAPolicy

    def __str__(self) -> str:
        status = "OK" if self.met else "BREACH"
        return (
            f"[{status}] {self.pipeline_id} "
            f"error_rate={self.actual_error_rate:.2%} "
            f"throughput={self.actual_throughput:.2f}/s"
        )


def evaluate_sla(
    pipeline_id: str,
    metrics: list[PipelineMetric],
    policy: Optional[SLAPolicy] = None,
) -> Optional[SLAResult]:
    """Evaluate SLA compliance for *pipeline_id* against *policy*.

    Returns ``None`` when there are no metrics to evaluate.
    """
    if not metrics:
        return None

    if policy is None:
        policy = SLAPolicy()

    effective = policy.for_pipeline(pipeline_id)

    er = error_rate(metrics)
    tp = throughput(metrics)

    error_rate_ok = er <= effective.max_error_rate
    throughput_ok = tp >= effective.min_throughput

    return SLAResult(
        pipeline_id=pipeline_id,
        met=error_rate_ok and throughput_ok,
        error_rate_ok=error_rate_ok,
        throughput_ok=throughput_ok,
        actual_error_rate=er,
        actual_throughput=tp,
        policy=effective,
    )
