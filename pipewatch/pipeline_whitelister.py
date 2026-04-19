from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class WhitelistEntry:
    pipeline_id: str
    reason: str

    def __str__(self) -> str:
        return f"WhitelistEntry(pipeline_id={self.pipeline_id}, reason={self.reason!r})"


@dataclass
class WhitelisterResult:
    entries: List[tuple]  # (snapshot, allowed)

    @property
    def total_allowed(self) -> int:
        return sum(1 for _, allowed in self.entries if allowed)

    @property
    def total_blocked(self) -> int:
        return sum(1 for _, allowed in self.entries if not allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        return [s.pipeline_id for s, _ in self.entries]


class PipelineWhitelister:
    def __init__(self) -> None:
        self._whitelist: Dict[str, WhitelistEntry] = {}

    def whitelist(self, pipeline_id: str, reason: str = "") -> WhitelistEntry:
        entry = WhitelistEntry(pipeline_id=pipeline_id, reason=reason)
        self._whitelist[pipeline_id] = entry
        return entry

    def unwhitelist(self, pipeline_id: str) -> None:
        self._whitelist.pop(pipeline_id, None)

    def is_whitelisted(self, pipeline_id: str) -> bool:
        return pipeline_id in self._whitelist

    @property
    def whitelisted_ids(self) -> List[str]:
        return list(self._whitelist.keys())

    def run(self, snapshots: List[PipelineSnapshot], default_allow: bool = False) -> WhitelisterResult:
        entries = []
        for snapshot in snapshots:
            allowed = self.is_whitelisted(snapshot.pipeline_id) or default_allow
            entries.append((snapshot, allowed))
        return WhitelisterResult(entries=entries)
