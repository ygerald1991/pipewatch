from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class LimitResult:
    pipeline_id: str
    snapshot_count: int
    limit: int
    limited: bool
    dropped: int

    def __str__(self) -> str:
        status = "LIMITED" if self.limited else "OK"
        return f"{self.pipeline_id}: {status} ({self.snapshot_count}/{self.limit}, dropped={self.dropped})"


@dataclass
class LimiterResult:
    results: List[LimitResult] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return [r.pipeline_id for r in self.results]

    def limited_pipelines(self) -> List[LimitResult]:
        return [r for r in self.results if r.limited]

    def total_dropped(self) -> int:
        return sum(r.dropped for r in self.results)


def limit_snapshots(
    snapshots: List[PipelineSnapshot],
    default_limit: int = 100,
    overrides: Optional[Dict[str, int]] = None,
) -> LimiterResult:
    overrides = overrides or {}
    grouped: Dict[str, List[PipelineSnapshot]] = {}
    for snap in snapshots:
        grouped.setdefault(snap.pipeline_id, []).append(snap)

    results = []
    for pid, snaps in grouped.items():
        limit = overrides.get(pid, default_limit)
        count = len(snaps)
        limited = count > limit
        dropped = max(0, count - limit)
        results.append(LimitResult(
            pipeline_id=pid,
            snapshot_count=min(count, limit),
            limit=limit,
            limited=limited,
            dropped=dropped,
        ))
    return LimiterResult(results=results)
