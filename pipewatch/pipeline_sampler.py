from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
import random

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class SampleResult:
    pipeline_id: str
    selected: List[PipelineSnapshot]
    total: int
    sample_size: int

    def __str__(self) -> str:
        return (
            f"SampleResult(pipeline={self.pipeline_id}, "
            f"sampled={self.sample_size}/{self.total})"
        )

    @property
    def coverage(self) -> float:
        if self.total == 0:
            return 0.0
        return self.sample_size / self.total


def sample_snapshots(
    snapshots: List[PipelineSnapshot],
    n: int,
    seed: Optional[int] = None,
) -> List[SampleResult]:
    """Sample up to n snapshots per pipeline from a mixed list."""
    by_pipeline: dict[str, List[PipelineSnapshot]] = {}
    for snap in snapshots:
        by_pipeline.setdefault(snap.pipeline_id, []).append(snap)

    rng = random.Random(seed)
    results: List[SampleResult] = []

    for pipeline_id, snaps in sorted(by_pipeline.items()):
        total = len(snaps)
        selected = rng.sample(snaps, min(n, total))
        results.append(
            SampleResult(
                pipeline_id=pipeline_id,
                selected=selected,
                total=total,
                sample_size=len(selected),
            )
        )

    return results
