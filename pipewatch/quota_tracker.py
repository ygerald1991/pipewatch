from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, throughput


@dataclass
class QuotaPolicy:
    """Defines throughput quotas per pipeline or globally."""

    default_max_throughput: float = 10_000.0
    overrides: Dict[str, float] = field(default_factory=dict)

    def max_throughput_for(self, pipeline_id: str) -> float:
        return self.overrides.get(pipeline_id, self.default_max_throughput)


@dataclass
class QuotaResult:
    pipeline_id: str
    current_throughput: float
    max_throughput: float
    exceeded: bool
    utilization: float  # 0.0 – 1.0

    def __str__(self) -> str:
        status = "EXCEEDED" if self.exceeded else "OK"
        pct = f"{self.utilization * 100:.1f}%"
        return (
            f"[{status}] {self.pipeline_id}: "
            f"{self.current_throughput:.1f}/{self.max_throughput:.1f} "
            f"({pct} utilization)"
        )


def evaluate_quota(
    pipeline_id: str,
    metrics: List[PipelineMetric],
    policy: QuotaPolicy,
) -> Optional[QuotaResult]:
    """Evaluate throughput quota for a single pipeline.

    Returns None when there are no metrics to evaluate.
    """
    if not metrics:
        return None

    current = throughput(metrics)
    maximum = policy.max_throughput_for(pipeline_id)
    utilization = current / maximum if maximum > 0 else 0.0
    exceeded = current > maximum

    return QuotaResult(
        pipeline_id=pipeline_id,
        current_throughput=current,
        max_throughput=maximum,
        exceeded=exceeded,
        utilization=utilization,
    )
