from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ReapResult:
    pipeline_id: str
    reason: str
    error_rate: Optional[float]
    throughput: Optional[float]
    reaped: bool

    def __str__(self) -> str:
        status = "REAPED" if self.reaped else "KEPT"
        return f"[{status}] {self.pipeline_id}: {self.reason}"


@dataclass
class ReaperResult:
    results: List[ReapResult] = field(default_factory=list)

    @property
    def reaped(self) -> List[ReapResult]:
        return [r for r in self.results if r.reaped]

    @property
    def kept(self) -> List[ReapResult]:
        return [r for r in self.results if not r.reaped]

    @property
    def total_reaped(self) -> int:
        return len(self.reaped)

    @property
    def pipeline_ids(self) -> List[str]:
        return [r.pipeline_id for r in self.results]


def reap_snapshots(
    snapshots: List[PipelineSnapshot],
    max_error_rate: float = 0.75,
    min_throughput: Optional[float] = None,
) -> ReaperResult:
    results: List[ReapResult] = []
    for snap in snapshots:
        er = snap.error_rate
        tp = snap.throughput
        reaped = False
        reason = "within limits"

        if er is not None and er >= max_error_rate:
            reaped = True
            reason = f"error_rate {er:.2%} >= threshold {max_error_rate:.2%}"
        elif min_throughput is not None and tp is not None and tp < min_throughput:
            reaped = True
            reason = f"throughput {tp:.2f} < min {min_throughput:.2f}"

        results.append(
            ReapResult(
                pipeline_id=snap.pipeline_id,
                reason=reason,
                error_rate=er,
                throughput=tp,
                reaped=reaped,
            )
        )
    return ReaperResult(results=results)
