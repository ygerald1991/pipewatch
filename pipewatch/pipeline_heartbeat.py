from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class HeartbeatEntry:
    pipeline_id: str
    last_seen: datetime
    interval_seconds: float

    def is_alive(self, now: Optional[datetime] = None) -> bool:
        if now is None:
            now = datetime.now(timezone.utc)
        elapsed = (now - self.last_seen).total_seconds()
        return elapsed <= self.interval_seconds

    def seconds_since(self, now: Optional[datetime] = None) -> float:
        if now is None:
            now = datetime.now(timezone.utc)
        return (now - self.last_seen).total_seconds()

    def __str__(self) -> str:
        status = "alive" if self.is_alive() else "dead"
        return f"{self.pipeline_id}: {status} (last seen {self.last_seen.isoformat()})"


@dataclass
class HeartbeatResult:
    total: int
    alive: List[HeartbeatEntry] = field(default_factory=list)
    dead: List[HeartbeatEntry] = field(default_factory=list)

    @property
    def total_alive(self) -> int:
        return len(self.alive)

    @property
    def total_dead(self) -> int:
        return len(self.dead)

    @property
    def all_alive(self) -> bool:
        return self.total_dead == 0


class PipelineHeartbeat:
    def __init__(self, default_interval: float = 60.0) -> None:
        self._default_interval = default_interval
        self._entries: Dict[str, HeartbeatEntry] = {}
        self._intervals: Dict[str, float] = {}

    def set_interval(self, pipeline_id: str, interval_seconds: float) -> None:
        self._intervals[pipeline_id] = interval_seconds

    def beat(self, pipeline_id: str, now: Optional[datetime] = None) -> HeartbeatEntry:
        if now is None:
            now = datetime.now(timezone.utc)
        interval = self._intervals.get(pipeline_id, self._default_interval)
        entry = HeartbeatEntry(
            pipeline_id=pipeline_id,
            last_seen=now,
            interval_seconds=interval,
        )
        self._entries[pipeline_id] = entry
        return entry

    def pipeline_ids(self) -> List[str]:
        return list(self._entries.keys())

    def check(self, now: Optional[datetime] = None) -> HeartbeatResult:
        if now is None:
            now = datetime.now(timezone.utc)
        alive, dead = [], []
        for entry in self._entries.values():
            if entry.is_alive(now):
                alive.append(entry)
            else:
                dead.append(entry)
        return HeartbeatResult(total=len(self._entries), alive=alive, dead=dead)

    def latest(self, pipeline_id: str) -> Optional[HeartbeatEntry]:
        return self._entries.get(pipeline_id)
