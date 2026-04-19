from __future__ import annotations
from typing import List

from pipewatch.pipeline_acknowledger import AcknowledgerResult, AckEntry


def _format_row(entry: AckEntry) -> str:
    ts = entry.acknowledged_at.strftime("%Y-%m-%d %H:%M:%S")
    reason = entry.reason if entry.reason else "-"
    return f"  {entry.pipeline_id:<30} {entry.acknowledged_by:<20} {ts}  {reason}"


def render_acknowledger_table(result: AcknowledgerResult) -> str:
    if not result.entries:
        return "No acknowledged pipelines."
    header = f"  {'PIPELINE':<30} {'ACKNOWLEDGED BY':<20} {'TIMESTAMP':<21} REASON"
    divider = "  " + "-" * 80
    rows = [_format_row(e) for e in result.entries]
    return "\n".join([header, divider] + rows)


def acknowledged_summary(result: AcknowledgerResult) -> str:
    total = result.total_acknowledged
    if total == 0:
        return "No pipelines acknowledged."
    ids = ", ".join(result.pipeline_ids)
    return f"{total} pipeline(s) acknowledged: {ids}"
