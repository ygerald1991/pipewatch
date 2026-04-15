"""Audit trail for pipeline state changes and significant events."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class AuditEvent:
    pipeline_id: str
    event_type: str
    detail: str
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def __str__(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] {self.pipeline_id} | {self.event_type} | {self.detail}"


class AuditLog:
    def __init__(self) -> None:
        self._events: List[AuditEvent] = []

    def record(self, event: AuditEvent) -> None:
        self._events.append(event)

    def events_for(self, pipeline_id: str) -> List[AuditEvent]:
        return [e for e in self._events if e.pipeline_id == pipeline_id]

    def all_events(self) -> List[AuditEvent]:
        return list(self._events)

    def clear(self) -> None:
        self._events.clear()

    def __len__(self) -> int:
        return len(self._events)


def audit_snapshot_change(
    log: AuditLog,
    previous: Optional[PipelineSnapshot],
    current: PipelineSnapshot,
) -> None:
    """Record audit events when a pipeline snapshot changes significantly."""
    pid = current.pipeline_id

    if previous is None:
        log.record(AuditEvent(
            pipeline_id=pid,
            event_type="SNAPSHOT_CREATED",
            detail=f"First snapshot recorded with {current.metric_count} metric(s)",
        ))
        return

    if previous.status != current.status:
        log.record(AuditEvent(
            pipeline_id=pid,
            event_type="STATUS_CHANGE",
            detail=f"Status changed from {previous.status.value} to {current.status.value}",
        ))

    prev_er = previous.error_rate
    curr_er = current.error_rate
    if prev_er is not None and curr_er is not None:
        delta = curr_er - prev_er
        if abs(delta) >= 0.05:
            direction = "increased" if delta > 0 else "decreased"
            log.record(AuditEvent(
                pipeline_id=pid,
                event_type="ERROR_RATE_SHIFT",
                detail=f"Error rate {direction} by {abs(delta):.2%} "
                       f"({prev_er:.2%} -> {curr_er:.2%})",
            ))

    prev_tp = previous.throughput
    curr_tp = current.throughput
    if prev_tp is not None and curr_tp is not None and prev_tp > 0:
        pct_change = (curr_tp - prev_tp) / prev_tp
        if abs(pct_change) >= 0.20:
            direction = "increased" if pct_change > 0 else "decreased"
            log.record(AuditEvent(
                pipeline_id=pid,
                event_type="THROUGHPUT_SHIFT",
                detail=f"Throughput {direction} by {abs(pct_change):.1%} "
                       f"({prev_tp:.1f} -> {curr_tp:.1f} rec/s)",
            ))
