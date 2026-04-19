from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DebounceEntry:
    pipeline_id: str
    first_seen: datetime
    last_seen: datetime
    trigger_count: int
    fired: bool

    def __str__(self) -> str:
        status = "fired" if self.fired else "pending"
        return f"DebounceEntry({self.pipeline_id}, count={self.trigger_count}, {status})"


@dataclass
class DebouncerResult:
    entries: List[DebounceEntry] = field(default_factory=list)

    @property
    def total_fired(self) -> int:
        return sum(1 for e in self.entries if e.fired)

    @property
    def total_pending(self) -> int:
        return sum(1 for e in self.entries if not e.fired)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]


class PipelineDebouncer:
    def __init__(self, window_seconds: float = 30.0, min_triggers: int = 3):
        self._window = timedelta(seconds=window_seconds)
        self._min_triggers = min_triggers
        self._state: Dict[str, DebounceEntry] = {}

    def trigger(self, pipeline_id: str, now: Optional[datetime] = None) -> DebounceEntry:
        now = now or datetime.utcnow()
        if pipeline_id not in self._state:
            entry = DebounceEntry(
                pipeline_id=pipeline_id,
                first_seen=now,
                last_seen=now,
                trigger_count=1,
                fired=False,
            )
            self._state[pipeline_id] = entry
        else:
            entry = self._state[pipeline_id]
            entry.last_seen = now
            entry.trigger_count += 1

        elapsed = entry.last_seen - entry.first_seen
        if entry.trigger_count >= self._min_triggers and elapsed <= self._window:
            entry.fired = True

        return entry

    def reset(self, pipeline_id: str) -> None:
        self._state.pop(pipeline_id, None)

    def run(self, snapshots: List[PipelineSnapshot]) -> DebouncerResult:
        results = []
        for snapshot in snapshots:
            entry = self.trigger(snapshot.pipeline_id)
            results.append(entry)
        return DebouncerResult(entries=results)

    @property
    def active_pipeline_ids(self) -> List[str]:
        return list(self._state.keys())
