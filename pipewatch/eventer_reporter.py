from pipewatch.pipeline_eventer import EventerResult


def _format_row(event) -> str:
    ts = event.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    return f"  {event.pipeline_id:<24} {event.event_type:<20} {ts}  {event.message}"


def render_eventer_table(result: EventerResult) -> str:
    if not result.events:
        return "No events recorded."
    header = f"  {'Pipeline':<24} {'Event Type':<20} {'Timestamp':<20} Message"
    separator = "  " + "-" * 80
    rows = [_format_row(e) for e in result.events]
    return "\n".join([header, separator] + rows)


def event_type_summary(result: EventerResult) -> str:
    if not result.events:
        return "No events."
    counts: dict = {}
    for e in result.events:
        counts[e.event_type] = counts.get(e.event_type, 0) + 1
    lines = [f"  {etype}: {count}" for etype, count in sorted(counts.items())]
    return "Event type summary:\n" + "\n".join(lines)
