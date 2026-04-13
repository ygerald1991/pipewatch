"""Snapshot module for capturing and comparing pipeline metric states."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetric, PipelineStatus, evaluate_status
from pipewatch.reporter import generate_report


@dataclass
class PipelineSnapshot:
    """A point-in-time snapshot of a pipeline's health state."""

    pipeline_id: str
    captured_at: datetime
    status: PipelineStatus
    error_rate: float
    throughput: float
    metric_count: int

    def is_degraded_compared_to(self, other: "PipelineSnapshot") -> bool:
        """Return True if this snapshot reflects worse health than another."""
        return self.status.value > other.status.value

    def to_dict(self) -> Dict:
        return {
            "pipeline_id": self.pipeline_id,
            "captured_at": self.captured_at.isoformat(),
            "status": self.status.value,
            "error_rate": self.error_rate,
            "throughput": self.throughput,
            "metric_count": self.metric_count,
        }


def capture_snapshot(
    pipeline_id: str, metrics: List[PipelineMetric]
) -> Optional[PipelineSnapshot]:
    """Capture a snapshot from a list of pipeline metrics."""
    if not metrics:
        return None

    report = generate_report(pipeline_id, metrics)
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        status=report.status,
        error_rate=report.avg_error_rate,
        throughput=report.avg_throughput,
        metric_count=len(metrics),
    )


@dataclass
class SnapshotStore:
    """Stores the latest snapshot per pipeline for change detection."""

    _snapshots: Dict[str, PipelineSnapshot] = field(default_factory=dict)

    def update(self, snapshot: PipelineSnapshot) -> bool:
        """Update snapshot; return True if status changed since last snapshot."""
        prev = self._snapshots.get(snapshot.pipeline_id)
        self._snapshots[snapshot.pipeline_id] = snapshot
        if prev is None:
            return False
        return prev.status != snapshot.status

    def get(self, pipeline_id: str) -> Optional[PipelineSnapshot]:
        return self._snapshots.get(pipeline_id)

    def all(self) -> List[PipelineSnapshot]:
        return list(self._snapshots.values())
