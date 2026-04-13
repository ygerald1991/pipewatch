"""Core metrics data structures for ETL pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class PipelineStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILING = "failing"
    UNKNOWN = "unknown"


@dataclass
class PipelineMetric:
    """Represents a single snapshot of pipeline health metrics."""

    pipeline_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    rows_processed: int = 0
    rows_failed: int = 0
    duration_seconds: float = 0.0
    lag_seconds: float = 0.0
    status: PipelineStatus = PipelineStatus.UNKNOWN
    error_message: Optional[str] = None

    @property
    def error_rate(self) -> float:
        """Calculate the error rate as a fraction of total rows."""
        total = self.rows_processed + self.rows_failed
        if total == 0:
            return 0.0
        return self.rows_failed / total

    @property
    def throughput(self) -> float:
        """Calculate rows processed per second."""
        if self.duration_seconds <= 0:
            return 0.0
        return self.rows_processed / self.duration_seconds

    def evaluate_status(self, max_error_rate: float = 0.05, max_lag_seconds: float = 300.0) -> PipelineStatus:
        """Evaluate and set pipeline status based on thresholds."""
        if self.error_message:
            self.status = PipelineStatus.FAILING
        elif self.error_rate > max_error_rate or self.lag_seconds > max_lag_seconds:
            self.status = PipelineStatus.DEGRADED
        elif self.rows_processed > 0:
            self.status = PipelineStatus.HEALTHY
        else:
            self.status = PipelineStatus.UNKNOWN
        return self.status

    def to_dict(self) -> dict:
        """Serialize metric to a plain dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "timestamp": self.timestamp.isoformat(),
            "rows_processed": self.rows_processed,
            "rows_failed": self.rows_failed,
            "duration_seconds": self.duration_seconds,
            "lag_seconds": self.lag_seconds,
            "error_rate": round(self.error_rate, 4),
            "throughput": round(self.throughput, 2),
            "status": self.status.value,
            "error_message": self.error_message,
        }
