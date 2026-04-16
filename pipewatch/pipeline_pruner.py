from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PruneResult:
    pipeline_id: str
    original_count: int
    pruned_count: int
    kept_count: int

    def __str__(self) -> str:
        return (
            f"PruneResult(pipeline={self.pipeline_id}, "
            f"original={self.original_count}, pruned={self.pruned_count}, "
            f"kept={self.kept_count})"
        )


@dataclass
class PrunerResult:
    results: List[PruneResult] = field(default_factory=list)

    @property
    def total_pruned(self) -> int:
        return sum(r.pruned_count for r in self.results)

    @property
    def total_kept(self) -> int:
        return sum(r.kept_count for r in self.results)

    def pipeline_ids(self) -> List[str]:
        return [r.pipeline_id for r in self.results]


def prune_snapshots(
    snapshots: List[PipelineSnapshot],
    max_per_pipeline: int = 10,
) -> PrunerResult:
    """Prune snapshots per pipeline, keeping only the most recent N."""
    grouped: Dict[str, List[PipelineSnapshot]] = {}
    for snap in snapshots:
        grouped.setdefault(snap.pipeline_id, []).append(snap)

    results: List[PruneResult] = []
    for pid, snaps in grouped.items():
        original = len(snaps)
        kept_snaps = sorted(snaps, key=lambda s: s.captured_at, reverse=True)[:max_per_pipeline]
        kept = len(kept_snaps)
        pruned = original - kept
        results.append(PruneResult(
            pipeline_id=pid,
            original_count=original,
            pruned_count=pruned,
            kept_count=kept,
        ))

    return PrunerResult(results=results)
