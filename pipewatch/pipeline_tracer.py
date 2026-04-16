from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class TraceSpan:
    pipeline_id: str
    stage: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    def __str__(self) -> str:
        dur = f"{self.duration_seconds:.2f}s" if self.duration_seconds is not None else "ongoing"
        return f"[{self.pipeline_id}] {self.stage} ({dur})"


@dataclass
class TraceResult:
    pipeline_id: str
    spans: List[TraceSpan] = field(default_factory=list)

    @property
    def total_duration(self) -> float:
        return sum(s.duration_seconds or 0.0 for s in self.spans)

    @property
    def stage_count(self) -> int:
        return len(self.spans)

    @property
    def has_incomplete_spans(self) -> bool:
        return any(s.ended_at is None for s in self.spans)


def trace_pipeline(pipeline_id: str, snapshots: List[PipelineSnapshot]) -> Optional[TraceResult]:
    if not snapshots:
        return None
    result = TraceResult(pipeline_id=pipeline_id)
    for i, snap in enumerate(snapshots):
        ts = snap.captured_at
        prev_ts = snapshots[i - 1].captured_at if i > 0 else ts
        span = TraceSpan(
            pipeline_id=pipeline_id,
            stage=f"stage_{i + 1}",
            started_at=prev_ts,
            ended_at=ts,
        )
        result.spans.append(span)
    return result
