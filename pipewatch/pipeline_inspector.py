from dataclasses import dataclass, field
from typing import Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class InspectionResult:
    pipeline_id: str
    snapshot_count: int
    avg_error_rate: Optional[float]
    avg_throughput: Optional[float]
    max_error_rate: Optional[float]
    min_throughput: Optional[float]
    flags: list = field(default_factory=list)

    def __str__(self) -> str:
        flags = ", ".join(self.flags) if self.flags else "none"
        avg_err = f"{self.avg_error_rate:.3f}" if self.avg_error_rate is not None else "N/A"
        avg_thr = f"{self.avg_throughput:.1f}" if self.avg_throughput is not None else "N/A"
        return (
            f"[{self.pipeline_id}] snapshots={self.snapshot_count} "
            f"avg_error={avg_err} avg_throughput={avg_thr} "
            f"flags=[{flags}]"
        )


def _safe_avg(values: list) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _safe_max(values: list) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


def _safe_min(values: list) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return min(vals) if vals else None


def inspect_pipeline(
    pipeline_id: str,
    snapshots: list,
    error_rate_warn: float = 0.05,
    throughput_warn: float = 10.0,
) -> Optional[InspectionResult]:
    if not snapshots:
        return None

    error_rates = [s.error_rate for s in snapshots]
    throughputs = [s.throughput for s in snapshots]

    avg_err = _safe_avg(error_rates)
    avg_thr = _safe_avg(throughputs)
    max_err = _safe_max(error_rates)
    min_thr = _safe_min(throughputs)

    flags = []
    if avg_err is not None and avg_err >= error_rate_warn:
        flags.append("high_error_rate")
    if avg_thr is not None and avg_thr < throughput_warn:
        flags.append("low_throughput")
    if max_err is not None and max_err >= 0.5:
        flags.append("critical_error_spike")

    return InspectionResult(
        pipeline_id=pipeline_id,
        snapshot_count=len(snapshots),
        avg_error_rate=avg_err,
        avg_throughput=avg_thr,
        max_error_rate=max_err,
        min_throughput=min_thr,
        flags=flags,
    )
