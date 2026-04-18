from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CapResult:
    pipeline_id: str
    original_count: int
    capped_count: int
    snapshots: List[PipelineSnapshot]

    def __str__(self) -> str:
        return (
            f"CapResult(pipeline={self.pipeline_id}, "
            f"original={self.original_count}, capped={self.capped_count})"
        )

    @property
    def was_capped(self) -> bool:
        return self.capped_count < self.original_count


@dataclass
class CapperResult:
    results: List[CapResult] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return [r.pipeline_id for r in self.results]

    @property
    def total_dropped(self) -> int:
        return sum(r.original_count - r.capped_count for r in self.results)

    def for_pipeline(self, pipeline_id: str) -> Optional[CapResult]:
        for r in self.results:
            if r.pipeline_id == pipeline_id:
                return r
        return None


def cap_snapshots(
    snapshots: List[PipelineSnapshot],
    max_per_pipeline: int = 10,
) -> CapperResult:
    grouped: dict[str, List[PipelineSnapshot]] = {}
    for snap in snapshots:
        grouped.setdefault(snap.pipeline_id, []).append(snap)

    results: List[CapResult] = []
    for pipeline_id, snaps in grouped.items():
        original = len(snaps)
        capped = snaps[:max_per_pipeline]
        results.append(
            CapResult(
                pipeline_id=pipeline_id,
                original_count=original,
                capped_count=len(capped),
                snapshots=capped,
            )
        )
    return CapperResult(results=results)
