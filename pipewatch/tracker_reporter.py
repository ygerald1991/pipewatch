from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_tracker import TrackerResult


def _format_row(pipeline_id: str, count: int, latest_label: str, latest_at: str) -> str:
    return f"  {pipeline_id:<30} {count:>6}  {latest_label:<20} {latest_at}"


def render_tracker_table(result: Optional[TrackerResult]) -> str:
    if result is None or result.total_tracked == 0:
        return "No tracked pipelines."

    lines = [
        f"{'Pipeline ID':<30} {'Entries':>6}  {'Latest Label':<20} Latest At",
        "-" * 80,
    ]

    for pid in sorted(result.pipeline_ids):
        entries = result.entries_for(pid)
        latest = result.latest_for(pid)
        latest_label = latest.label if latest and latest.label else "-"
        latest_at = latest.tracked_at.isoformat() if latest else "-"
        lines.append(_format_row(pid, len(entries), latest_label, latest_at))

    return "\n".join(lines)


def tracked_summary(result: Optional[TrackerResult]) -> str:
    if result is None or result.total_tracked == 0:
        return "Tracked: 0 entries across 0 pipelines."
    pipeline_count = len(result.pipeline_ids)
    return (
        f"Tracked: {result.total_tracked} entr"
        f"{'y' if result.total_tracked == 1 else 'ies'} "
        f"across {pipeline_count} pipeline"
        f"{'s' if pipeline_count != 1 else ''}."
    )
