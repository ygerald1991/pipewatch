"""Rank pipelines by composite health score, error rate, and throughput."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class RankEntry:
    pipeline_id: str
    rank: int
    error_rate: Optional[float]
    throughput: Optional[float]
    composite_score: float

    def __str__(self) -> str:
        er = f"{self.error_rate:.4f}" if self.error_rate is not None else "n/a"
        tp = f"{self.throughput:.2f}" if self.throughput is not None else "n/a"
        return (
            f"#{self.rank} {self.pipeline_id} "
            f"score={self.composite_score:.3f} error_rate={er} throughput={tp}"
        )


@dataclass
class RankingResult:
    entries: List[RankEntry] = field(default_factory=list)

    def top(self, n: int = 5) -> List[RankEntry]:
        return self.entries[:n]

    def bottom(self, n: int = 5) -> List[RankEntry]:
        return self.entries[-n:]

    def for_pipeline(self, pipeline_id: str) -> Optional[RankEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None


def _composite_score(snapshot: PipelineSnapshot) -> float:
    """Lower score is worse (higher error rate, lower throughput)."""
    error_penalty = (snapshot.error_rate or 0.0) * 100.0
    throughput_bonus = min((snapshot.avg_throughput or 0.0) / 100.0, 1.0) * 10.0
    return max(0.0, 100.0 - error_penalty + throughput_bonus)


def rank_pipelines(snapshots: List[PipelineSnapshot]) -> RankingResult:
    """Rank pipelines from best to worst by composite score."""
    if not snapshots:
        return RankingResult()

    scored = [
        (snap, _composite_score(snap))
        for snap in snapshots
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    entries = [
        RankEntry(
            pipeline_id=snap.pipeline_id,
            rank=idx + 1,
            error_rate=snap.error_rate,
            throughput=snap.avg_throughput,
            composite_score=score,
        )
        for idx, (snap, score) in enumerate(scored)
    ]
    return RankingResult(entries=entries)
