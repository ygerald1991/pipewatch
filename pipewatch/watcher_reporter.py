from __future__ import annotations
from pipewatch.pipeline_watcher import WatcherResult


def _format_row(pipeline_id: str, status: str, notes: str) -> str:
    return f"  {pipeline_id:<30} {status:<12} {notes}"


def render_watcher_table(result: WatcherResult) -> str:
    if not result.entries:
        return "No pipelines registered for watching."

    lines = [
        f"{'Pipeline':<30} {'Status':<12} Notes",
        "-" * 60,
    ]
    for entry in result.entries:
        status = "watching" if entry.enabled else "paused"
        lines.append(_format_row(entry.pipeline_id, status, entry.notes))

    lines.append("-" * 60)
    lines.append(f"Watching: {result.total_watching}  Paused: {result.total_paused}")
    return "\n".join(lines)


def watching_summary(result: WatcherResult) -> str:
    if result.total_watching == 0:
        return "No active watches."
    ids = [e.pipeline_id for e in result.entries if e.enabled]
    return "Actively watching: " + ", ".join(ids)
