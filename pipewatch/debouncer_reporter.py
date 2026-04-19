from pipewatch.pipeline_debouncer import DebouncerResult


def _format_row(entry) -> str:
    status = "FIRED" if entry.fired else "pending"
    return f"  {entry.pipeline_id:<30} triggers={entry.trigger_count:<5} {status}"


def render_debouncer_table(result: DebouncerResult) -> str:
    if not result.entries:
        return "No debounce entries."

    lines = ["Pipeline Debouncer Report", "-" * 50]
    for entry in result.entries:
        lines.append(_format_row(entry))
    lines.append("-" * 50)
    lines.append(f"Fired: {result.total_fired}  Pending: {result.total_pending}")
    return "\n".join(lines)


def fired_summary(result: DebouncerResult) -> str:
    fired = [e.pipeline_id for e in result.entries if e.fired]
    if not fired:
        return "No pipelines have fired debounce threshold."
    return "Fired: " + ", ".join(fired)
