from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class BlacklistEntry:
    pipeline_id: str
    reason: str
    added_at: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        return f"BlacklistEntry({self.pipeline_id}, reason={self.reason!r})"


@dataclass
class BlacklisterResult:
    entries: List[BlacklistEntry]
    allowed: List[PipelineSnapshot]

    @property
    def total_blocked(self) -> int:
        return len(self.entries)

    @property
    def total_allowed(self) -> int:
        return len(self.allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineBlacklister:
    def __init__(self) -> None:
        self._blacklist: Dict[str, BlacklistEntry] = {}

    def blacklist(self, pipeline_id: str, reason: str = "") -> BlacklistEntry:
        entry = BlacklistEntry(pipeline_id=pipeline_id, reason=reason)
        self._blacklist[pipeline_id] = entry
        return entry

    def unblacklist(self, pipeline_id: str) -> bool:
        if pipeline_id in self._blacklist:
            del self._blacklist[pipeline_id]
            return True
        return False

    def is_blacklisted(self, pipeline_id: str) -> bool:
        return pipeline_id in self._blacklist

    def apply(self, snapshots: List[PipelineSnapshot]) -> BlacklisterResult:
        blocked: List[BlacklistEntry] = []
        allowed: List[PipelineSnapshot] = []
        for snap in snapshots:
            if snap.pipeline_id in self._blacklist:
                blocked.append(self._blacklist[snap.pipeline_id])
            else:
                allowed.append(snap)
        return BlacklisterResult(entries=blocked, allowed=allowed)

    @property
    def blacklisted_ids(self) -> List[str]:
        return list(self._blacklist.keys())
