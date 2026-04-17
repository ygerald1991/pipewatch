from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class SplitResult:
    bucket: str
    snapshots: List[PipelineSnapshot] = field(default_factory=list)

    def __str__(self) -> str:
        return f"SplitResult(bucket={self.bucket!r}, count={len(self.snapshots)})"

    @property
    def size(self) -> int:
        return len(self.snapshots)

    @property
    def pipeline_ids(self) -> List[str]:
        return [s.pipeline_id for s in self.snapshots]


@dataclass
class SplitterResult:
    buckets: Dict[str, SplitResult] = field(default_factory=dict)

    def bucket_names(self) -> List[str]:
        return list(self.buckets.keys())

    def for_bucket(self, name: str) -> Optional[SplitResult]:
        return self.buckets.get(name)

    @property
    def total(self) -> int:
        return sum(r.size for r in self.buckets.values())


def split_snapshots(
    snapshots: List[PipelineSnapshot],
    key_fn: Callable[[PipelineSnapshot], str],
    default_bucket: str = "default",
) -> SplitterResult:
    result = SplitterResult()
    for snapshot in snapshots:
        try:
            bucket = key_fn(snapshot)
        except Exception:
            bucket = default_bucket
        if bucket not in result.buckets:
            result.buckets[bucket] = SplitResult(bucket=bucket)
        result.buckets[bucket].snapshots.append(snapshot)
    return result
