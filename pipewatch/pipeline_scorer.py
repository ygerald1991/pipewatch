"""Aggregate scoring across multiple pipelines for ranking and prioritization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.health_score import HealthScore, compute_health_score
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PipelineScore:
    pipeline_id: str
    health: HealthScore
    rank: int = 0

    def __str__(self) -> str:
        return f"{self.pipeline_id} rank={self.rank} score={self.health.score:.1f} ({self.health.grade})"


@dataclass
class ScoringResult:
    scores: List[PipelineScore] = field(default_factory=list)

    def top(self, n: int = 5) -> List[PipelineScore]:
        """Return top N healthiest pipelines."""
        return self.scores[:n]

    def bottom(self, n: int = 5) -> List[PipelineScore]:
        """Return bottom N least healthy pipelines."""
        return self.scores[-n:]

    def by_id(self, pipeline_id: str) -> Optional[PipelineScore]:
        for s in self.scores:
            if s.pipeline_id == pipeline_id:
                return s
        return None


def score_pipelines(snapshots: Dict[str, PipelineSnapshot]) -> ScoringResult:
    """Compute and rank health scores for all pipelines.

    Pipelines are ranked from healthiest (rank 1) to least healthy.
    Pipelines with no metrics are excluded from results.
    """
    raw: List[PipelineScore] = []

    for pipeline_id, snapshot in snapshots.items():
        health = compute_health_score(snapshot.metrics)
        if health is None:
            continue
        raw.append(PipelineScore(pipeline_id=pipeline_id, health=health))

    raw.sort(key=lambda s: s.health.score, reverse=True)

    for rank, entry in enumerate(raw, start=1):
        entry.rank = rank

    return ScoringResult(scores=raw)
