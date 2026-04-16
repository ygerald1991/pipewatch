from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ClassificationResult:
    pipeline_id: str
    category: str
    reason: str
    error_rate: Optional[float]
    throughput: Optional[float]

    def __str__(self) -> str:
        return f"{self.pipeline_id}: {self.category} ({self.reason})"


_CATEGORIES = [
    ("critical", lambda e, t: e is not None and e >= 0.20),
    ("degraded", lambda e, t: e is not None and e >= 0.10),
    ("idle",     lambda e, t: t is not None and t == 0.0),
    ("healthy",  lambda e, t: True),
]

_REASONS = {
    "critical": "error rate >= 20%",
    "degraded": "error rate >= 10%",
    "idle":     "zero throughput",
    "healthy":  "within normal bounds",
}


def classify_snapshot(snapshot: PipelineSnapshot) -> ClassificationResult:
    e = snapshot.error_rate
    t = snapshot.throughput
    for category, predicate in _CATEGORIES:
        if predicate(e, t):
            return ClassificationResult(
                pipeline_id=snapshot.pipeline_id,
                category=category,
                reason=_REASONS[category],
                error_rate=e,
                throughput=t,
            )
    return ClassificationResult(
        pipeline_id=snapshot.pipeline_id,
        category="unknown",
        reason="no data",
        error_rate=e,
        throughput=t,
    )


def classify_snapshots(
    snapshots: list[PipelineSnapshot],
) -> list[ClassificationResult]:
    return [classify_snapshot(s) for s in snapshots]
