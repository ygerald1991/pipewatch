from pipewatch.pipeline_auditor import AuditLog, AuditEvent
from typing import List


def _format_row(event: AuditEvent) -> str:
    ts = event.timestamp.strftime("%Y-%m-%d %H:%M:%S") if hasattr(event.timestamp, "strftime") else str(event.timestamp)
    return f"  {event.pipeline_id:<20} {event.event_type:<25} {ts}  {event.message}"


def render_audit_table(log: AuditLog, pipeline_id: str = None) -> str:
    if pipeline_id:
        events = log.events_for(pipeline_id)
        header = f"Audit Log — {pipeline_id}"
    else:
        events = log.all_events()
        header = "Audit Log — All Pipelines"

    if not events:
        return f"{header}\n  (no events recorded)"

    lines = [header, "-" * 80]
    lines.append(f"  {'Pipeline':<20} {'Event Type':<25} {'Timestamp':<21} Message")
    lines.append("-" * 80)
    for event in events:
        lines.append(_format_row(event))
    lines.append("-" * 80)
    lines.append(f"  Total events: {len(events)}")
    return "\n".join(lines)


def event_type_summary(log: AuditLog) -> str:
    counts: dict = {}
    for event in log.all_events():
        counts[event.event_type] = counts.get(event.event_type, 0) + 1
    if not counts:
        return "No events recorded."
    lines = ["Event Type Summary:"]
    for etype, count in sorted(counts.items()):
        lines.append(f"  {etype:<30} {count}")
    return "\n".join(lines)
