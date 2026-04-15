"""Checkpoint tracking for ETL pipeline runs."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List


@dataclass
class Checkpoint:
    pipeline_id: str
    stage: str
    timestamp: datetime
    success: bool
    message: str = ""

    def __str__(self) -> str:
        status = "OK" if self.success else "FAIL"
        ts = self.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        return f"[{ts}] {self.pipeline_id}/{self.stage}: {status}" + (
            f" — {self.message}" if self.message else ""
        )


@dataclass
class CheckpointStore:
    _records: Dict[str, List[Checkpoint]] = field(default_factory=dict)

    def record(self, checkpoint: Checkpoint) -> None:
        self._records.setdefault(checkpoint.pipeline_id, []).append(checkpoint)

    def latest(self, pipeline_id: str) -> Optional[Checkpoint]:
        records = self._records.get(pipeline_id, [])
        return records[-1] if records else None

    def history(self, pipeline_id: str) -> List[Checkpoint]:
        return list(self._records.get(pipeline_id, []))

    def pipeline_ids(self) -> List[str]:
        return list(self._records.keys())

    def last_failure(self, pipeline_id: str) -> Optional[Checkpoint]:
        for cp in reversed(self._records.get(pipeline_id, [])):
            if not cp.success:
                return cp
        return None

    def all_passed(self, pipeline_id: str) -> bool:
        records = self._records.get(pipeline_id, [])
        return bool(records) and all(cp.success for cp in records)
