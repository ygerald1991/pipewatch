from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class AckEntry:
    pipeline_id: str
    acknowledged_at: datetime
    acknowledged_by: str
    reason: str

    def __str__(self) -> str:
        ts = self.acknowledged_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"{self.pipeline_id} acked by {self.acknowledged_by} at {ts}: {self.reason}"


@dataclass
class AcknowledgerResult:
    entries: List[AckEntry] = field(default_factory=list)

    @property
    def total_acknowledged(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[AckEntry]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None


class PipelineAcknowledger:
    def __init__(self) -> None:
        self._acks: Dict[str, AckEntry] = {}

    def acknowledge(self, pipeline_id: str, acknowledged_by: str, reason: str = "") -> AckEntry:
        entry = AckEntry(
            pipeline_id=pipeline_id,
            acknowledged_at=datetime.utcnow(),
            acknowledged_by=acknowledged_by,
            reason=reason,
        )
        self._acks[pipeline_id] = entry
        return entry

    def unacknowledge(self, pipeline_id: str) -> bool:
        if pipeline_id in self._acks:
            del self._acks[pipeline_id]
            return True
        return False

    def is_acknowledged(self, pipeline_id: str) -> bool:
        return pipeline_id in self._acks

    def get(self, pipeline_id: str) -> Optional[AckEntry]:
        return self._acks.get(pipeline_id)

    def run(self, snapshots: List[PipelineSnapshot]) -> AcknowledgerResult:
        entries = []
        for snap in snapshots:
            entry = self._acks.get(snap.pipeline_id)
            if entry:
                entries.append(entry)
        return AcknowledgerResult(entries=entries)

    @property
    def acknowledged_pipeline_ids(self) -> List[str]:
        return list(self._acks.keys())
