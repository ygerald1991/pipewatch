from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DivertEntry:
    pipeline_id: str
    original_destination: str
    diverted_to: str
    reason: str

    def __str__(self) -> str:
        return (
            f"DivertEntry(pipeline={self.pipeline_id}, "
            f"from={self.original_destination}, to={self.diverted_to}, "
            f"reason={self.reason})"
        )


@dataclass
class DiverterResult:
    entries: List[DivertEntry] = field(default_factory=list)

    @property
    def total_diverted(self) -> int:
        return sum(1 for e in self.entries if e.diverted_to != e.original_destination)

    @property
    def total_unchanged(self) -> int:
        return len(self.entries) - self.total_diverted

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[DivertEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None


DivertCondition = Callable[[PipelineSnapshot], bool]


@dataclass
class DivertRule:
    condition: DivertCondition
    destination: str
    reason: str

    def matches(self, snapshot: PipelineSnapshot) -> bool:
        try:
            return self.condition(snapshot)
        except Exception:
            return False


def divert_snapshots(
    snapshots: List[PipelineSnapshot],
    rules: List[DivertRule],
    default_destination: str = "default",
) -> DiverterResult:
    entries: List[DivertEntry] = []
    for snapshot in snapshots:
        destination = default_destination
        reason = "no rule matched"
        for rule in rules:
            if rule.matches(snapshot):
                destination = rule.destination
                reason = rule.reason
                break
        entries.append(
            DivertEntry(
                pipeline_id=snapshot.pipeline_id,
                original_destination=default_destination,
                diverted_to=destination,
                reason=reason,
            )
        )
    return DiverterResult(entries=entries)
