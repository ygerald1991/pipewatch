from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class StageResult:
    pipeline_id: str
    stage: str
    snapshot_count: int
    avg_error_rate: Optional[float]
    avg_throughput: Optional[float]

    def __str__(self) -> str:
        er = f"{self.avg_error_rate:.3f}" if self.avg_error_rate is not None else "n/a"
        tp = f"{self.avg_throughput:.1f}" if self.avg_throughput is not None else "n/a"
        return f"[{self.stage}] {self.pipeline_id}: error_rate={er} throughput={tp} n={self.snapshot_count}"


@dataclass
class StagerResult:
    stages: List[StageResult] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return sorted({r.pipeline_id for r in self.stages})

    def for_stage(self, stage: str) -> List[StageResult]:
        return [r for r in self.stages if r.stage == stage]

    def stage_names(self) -> List[str]:
        """Return a sorted list of unique stage names present in the results."""
        return sorted({r.stage for r in self.stages})


def _safe_avg(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def stage_snapshots(
    snapshots: List[PipelineSnapshot],
    stage_map: dict,
    default_stage: str = "default",
) -> StagerResult:
    """Group snapshots by stage and compute per-stage statistics.

    stage_map maps pipeline_id -> stage name.
    """
    from collections import defaultdict

    buckets: dict = defaultdict(list)
    for snap in snapshots:
        stage = stage_map.get(snap.pipeline_id, default_stage)
        buckets[(snap.pipeline_id, stage)].append(snap)

    results = []
    for (pid, stage), snaps in buckets.items():
        error_rates = [s.error_rate for s in snaps if s.error_rate is not None]
        throughputs = [s.throughput for s in snaps if s.throughput is not None]
        results.append(
            StageResult(
                pipeline_id=pid,
                stage=stage,
                snapshot_count=len(snaps),
                avg_error_rate=_safe_avg(error_rates),
                avg_throughput=_safe_avg(throughputs),
            )
        )

    results.sort(key=lambda r: (r.stage, r.pipeline_id))
    return StagerResult(stages=results)
