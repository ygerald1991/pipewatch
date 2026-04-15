"""Archive pipeline snapshots to persistent storage with rotation support."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ArchiveEntry:
    pipeline_id: str
    archived_at: str
    snapshot: Dict

    def to_dict(self) -> Dict:
        return {
            "pipeline_id": self.pipeline_id,
            "archived_at": self.archived_at,
            "snapshot": self.snapshot,
        }


@dataclass
class ArchiveStore:
    _entries: Dict[str, List[ArchiveEntry]] = field(default_factory=dict)
    max_entries_per_pipeline: int = 50

    def archive(self, snapshot: PipelineSnapshot) -> ArchiveEntry:
        pid = snapshot.pipeline_id
        entry = ArchiveEntry(
            pipeline_id=pid,
            archived_at=datetime.now(timezone.utc).isoformat(),
            snapshot=snapshot.to_dict(),
        )
        bucket = self._entries.setdefault(pid, [])
        bucket.append(entry)
        if len(bucket) > self.max_entries_per_pipeline:
            bucket.pop(0)
        return entry

    def entries_for(self, pipeline_id: str) -> List[ArchiveEntry]:
        return list(self._entries.get(pipeline_id, []))

    def latest_for(self, pipeline_id: str) -> Optional[ArchiveEntry]:
        bucket = self._entries.get(pipeline_id, [])
        return bucket[-1] if bucket else None

    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())

    def total_entries(self) -> int:
        return sum(len(v) for v in self._entries.values())

    def prune(self, pipeline_id: str, keep: int) -> int:
        bucket = self._entries.get(pipeline_id, [])
        removed = max(0, len(bucket) - keep)
        self._entries[pipeline_id] = bucket[-keep:] if keep > 0 else []
        return removed

    def export_json(self, pipeline_id: str) -> str:
        entries = self.entries_for(pipeline_id)
        return json.dumps([e.to_dict() for e in entries], indent=2)
