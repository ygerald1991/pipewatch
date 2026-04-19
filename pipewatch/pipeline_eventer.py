from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PipelineEvent:
    pipeline_id: str
    event_type: str
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)

    def __str__(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        return f"[{ts}] {self.pipeline_id} | {self.event_type} | {self.message}"


@dataclass
class EventerResult:
    events: List[PipelineEvent] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return list({e.pipeline_id for e in self.events})

    def events_for(self, pipeline_id: str) -> List[PipelineEvent]:
        return [e for e in self.events if e.pipeline_id == pipeline_id]

    def of_type(self, event_type: str) -> List[PipelineEvent]:
        return [e for e in self.events if e.event_type == event_type]

    @property
    def total(self) -> int:
        return len(self.events)


def emit_event(
    snapshot: PipelineSnapshot,
    event_type: str,
    message: str,
    metadata: Optional[dict] = None,
) -> PipelineEvent:
    return PipelineEvent(
        pipeline_id=snapshot.pipeline_id,
        event_type=event_type,
        message=message,
        metadata=metadata or {},
    )


def emit_events_from_snapshots(
    snapshots: List[PipelineSnapshot],
    event_type: str,
    message_fn=None,
) -> EventerResult:
    if message_fn is None:
        message_fn = lambda s: f"Event for {s.pipeline_id}"
    events = [emit_event(s, event_type, message_fn(s)) for s in snapshots]
    return EventerResult(events=events)
