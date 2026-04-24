from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class BalanceEntry:
    pipeline_id: str
    assigned_slot: int
    load_score: float
    overloaded: bool

    def __str__(self) -> str:
        state = "OVERLOADED" if self.overloaded else "OK"
        return f"{self.pipeline_id} slot={self.assigned_slot} load={self.load_score:.3f} [{state}]"


@dataclass
class BalancerResult:
    entries: List[BalanceEntry] = field(default_factory=list)

    @property
    def total_overloaded(self) -> int:
        return sum(1 for e in self.entries if e.overloaded)

    @property
    def total_balanced(self) -> int:
        return sum(1 for e in self.entries if not e.overloaded)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[BalanceEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None


def _load_score(snapshot: PipelineSnapshot) -> float:
    """Compute a normalised load score from error_rate and throughput."""
    error_rate = snapshot.error_rate or 0.0
    throughput = snapshot.throughput or 0.0
    return round(error_rate * 0.7 + min(throughput / 1000.0, 1.0) * 0.3, 4)


def balance_snapshots(
    snapshots: List[PipelineSnapshot],
    num_slots: int = 4,
    overload_threshold: float = 0.6,
) -> BalancerResult:
    """Distribute pipelines across slots by ascending load score."""
    if not snapshots:
        return BalancerResult()

    scored = sorted(snapshots, key=lambda s: _load_score(s))
    entries: List[BalanceEntry] = []

    for idx, snapshot in enumerate(scored):
        slot = (idx % num_slots) + 1
        score = _load_score(snapshot)
        entries.append(
            BalanceEntry(
                pipeline_id=snapshot.pipeline_id,
                assigned_slot=slot,
                load_score=score,
                overloaded=score >= overload_threshold,
            )
        )

    return BalancerResult(entries=entries)
