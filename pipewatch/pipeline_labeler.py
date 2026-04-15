"""Assigns human-readable labels to pipelines based on their health and status."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


@dataclass
class PipelineLabel:
    pipeline_id: str
    labels: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tag_str = ", ".join(self.labels) if self.labels else "(none)"
        return f"{self.pipeline_id}: [{tag_str}]"


def _status_label(snapshot: PipelineSnapshot) -> str:
    status_map = {
        PipelineStatus.HEALTHY: "healthy",
        PipelineStatus.WARNING: "warning",
        PipelineStatus.CRITICAL: "critical",
    }
    return status_map.get(snapshot.status, "unknown")


def _error_rate_label(snapshot: PipelineSnapshot) -> Optional[str]:
    if snapshot.error_rate is None:
        return None
    if snapshot.error_rate >= 0.5:
        return "high-error-rate"
    if snapshot.error_rate >= 0.1:
        return "elevated-error-rate"
    return None


def _throughput_label(snapshot: PipelineSnapshot) -> Optional[str]:
    if snapshot.throughput is None:
        return None
    if snapshot.throughput == 0.0:
        return "idle"
    if snapshot.throughput >= 1000.0:
        return "high-throughput"
    return None


def label_snapshot(snapshot: PipelineSnapshot) -> PipelineLabel:
    """Derive descriptive labels from a single pipeline snapshot."""
    labels: List[str] = [_status_label(snapshot)]

    error_label = _error_rate_label(snapshot)
    if error_label:
        labels.append(error_label)

    throughput_label = _throughput_label(snapshot)
    if throughput_label:
        labels.append(throughput_label)

    return PipelineLabel(pipeline_id=snapshot.pipeline_id, labels=labels)


def label_snapshots(snapshots: List[PipelineSnapshot]) -> Dict[str, PipelineLabel]:
    """Return a mapping of pipeline_id -> PipelineLabel for all snapshots."""
    return {s.pipeline_id: label_snapshot(s) for s in snapshots}
