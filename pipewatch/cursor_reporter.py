from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_cursor import CursorResult, CursorEntry


def _format_row(entry: CursorEntry) -> str:
    status = "END" if not entry.has_next else "OK"
    return f"  {entry.pipeline_id:<30} {entry.position:>5} / {entry.total:<5} {entry.progress_pct:>6.1f}%  [{status}]"


def render_cursor_table(result: Optional[CursorResult]) -> str:
    if result is None or not result.entries:
        return "No cursor data available."

    lines = [
        "Pipeline Cursor Positions",
        "-" * 60,
        f"  {'Pipeline':<30} {'Pos':>5}   {'Total':<5} {'Progress':>8}   Status",
        "-" * 60,
    ]
    for entry in sorted(result.entries, key=lambda e: e.pipeline_id):
        lines.append(_format_row(entry))
    lines.append("-" * 60)
    lines.append(f"  Total pipelines tracked: {result.total_pipelines}")
    at_end = result.at_end()
    if at_end:
        lines.append(f"  Pipelines at end: {', '.join(e.pipeline_id for e in at_end)}")
    return "\n".join(lines)


def cursor_progress_summary(result: Optional[CursorResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines tracked."
    at_end = result.at_end()
    in_progress = [e for e in result.entries if e.has_next]
    parts = [f"{len(result.entries)} pipeline(s) tracked"]
    if in_progress:
        parts.append(f"{len(in_progress)} in progress")
    if at_end:
        parts.append(f"{len(at_end)} at end")
    return ", ".join(parts) + "."
