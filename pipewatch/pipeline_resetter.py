from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ResetEntry:
    pipeline_id: str
    reason: str
    previous_error_rate: Optional[float]
    previous_throughput: Optional[float]

    def __str__(self) -> str:
        return f"ResetEntry(pipeline={self.pipeline_id}, reason={self.reason})"


@dataclass
class ResetterResult:
    entries: List[ResetEntry] = field(default_factory=list)

    @property
    def total_reset(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[ResetEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None


def reset_snapshots(
    snapshots: List[PipelineSnapshot],
    reason: str = "manual_reset",
    only_degraded: bool = False,
) -> ResetterResult:
    result = ResetterResult()
    for snapshot in snapshots:
        if only_degraded and snapshot.error_rate is not None and snapshot.error_rate < 0.1:
            continue
        entry = ResetEntry(
            pipeline_id=snapshot.pipeline_id,
            reason=reason,
            previous_error_rate=snapshot.error_rate,
            previous_throughput=snapshot.throughput,
        )
        result.entries.append(entry)
    return result
