"""Drift detection: compare current pipeline snapshots against a baseline."""

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DriftResult:
    pipeline_id: str
    has_drift: bool
    error_rate_delta: float
    throughput_delta: float
    details: str = ""

    def __str__(self) -> str:
        if not self.has_drift:
            return f"[{self.pipeline_id}] No drift detected."
        return (
            f"[{self.pipeline_id}] Drift detected: "
            f"error_rate delta={self.error_rate_delta:+.4f}, "
            f"throughput delta={self.throughput_delta:+.2f}. "
            f"{self.details}"
        )


def detect_drift(
    baseline: PipelineSnapshot,
    current: PipelineSnapshot,
    error_rate_threshold: float = 0.05,
    throughput_threshold: float = 0.20,
) -> DriftResult:
    """Return a DriftResult comparing *current* against *baseline*.

    Drift is flagged when the absolute change in error_rate exceeds
    *error_rate_threshold* OR the relative drop in throughput exceeds
    *throughput_threshold* (e.g. 0.20 == 20 %).
    """
    er_delta = current.error_rate - baseline.error_rate
    tp_delta = current.throughput - baseline.throughput

    relative_tp_drop = (
        (baseline.throughput - current.throughput) / baseline.throughput
        if baseline.throughput > 0
        else 0.0
    )

    reasons = []
    if abs(er_delta) > error_rate_threshold:
        reasons.append(
            f"error_rate changed by {er_delta:+.4f} (threshold ±{error_rate_threshold})"
        )
    if relative_tp_drop > throughput_threshold:
        reasons.append(
            f"throughput dropped {relative_tp_drop:.1%} (threshold {throughput_threshold:.1%})"
        )

    has_drift = bool(reasons)
    return DriftResult(
        pipeline_id=current.pipeline_id,
        has_drift=has_drift,
        error_rate_delta=er_delta,
        throughput_delta=tp_delta,
        details="; ".join(reasons),
    )
