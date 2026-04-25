from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ClampEntry:
    pipeline_id: str
    original_error_rate: Optional[float]
    clamped_error_rate: Optional[float]
    original_throughput: Optional[float]
    clamped_throughput: Optional[float]

    def __str__(self) -> str:
        return (
            f"ClampEntry(pipeline={self.pipeline_id}, "
            f"error_rate={self.original_error_rate}->{self.clamped_error_rate}, "
            f"throughput={self.original_throughput}->{self.clamped_throughput})"
        )


@dataclass
class ClamperResult:
    entries: List[ClampEntry] = field(default_factory=list)

    @property
    def total_clamped(self) -> int:
        return sum(
            1 for e in self.entries
            if e.original_error_rate != e.clamped_error_rate
            or e.original_throughput != e.clamped_throughput
        )

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[ClampEntry]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None


def _clamp(value: Optional[float], lo: float, hi: float) -> Optional[float]:
    if value is None:
        return None
    return max(lo, min(hi, value))


def clamp_snapshots(
    snapshots: List[PipelineSnapshot],
    min_error_rate: float = 0.0,
    max_error_rate: float = 1.0,
    min_throughput: float = 0.0,
    max_throughput: Optional[float] = None,
) -> ClamperResult:
    entries: List[ClampEntry] = []
    for snap in snapshots:
        orig_er = snap.error_rate
        orig_tp = snap.throughput
        hi_tp = max_throughput if max_throughput is not None else float("inf")
        clamped_er = _clamp(orig_er, min_error_rate, max_error_rate)
        clamped_tp = _clamp(orig_tp, min_throughput, hi_tp)
        entries.append(
            ClampEntry(
                pipeline_id=snap.pipeline_id,
                original_error_rate=orig_er,
                clamped_error_rate=clamped_er,
                original_throughput=orig_tp,
                clamped_throughput=clamped_tp,
            )
        )
    return ClamperResult(entries=entries)
