from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_inhibitor import InhibitorResult, PipelineInhibitor


def _format_row(pipeline_id: str, reason: str, inhibited_by: str, inhibited_at: str) -> str:
    return f"  {pipeline_id:<30} {inhibited_by:<20} {reason:<40} {inhibited_at}"


def render_inhibitor_table(inhibitor: PipelineInhibitor) -> str:
    ids = inhibitor.inhibited_pipeline_ids
    if not ids:
        return "No inhibited pipelines."

    header = _format_row("PIPELINE", "REASON", "INHIBITED BY", "INHIBITED AT")
    separator = "-" * len(header)
    lines = ["Inhibited Pipelines", separator, header, separator]

    for pid in ids:
        entry = inhibitor.entry_for(pid)
        if entry:
            ts = entry.inhibited_at.strftime("%Y-%m-%dT%H:%M:%SZ")
            lines.append(_format_row(pid, entry.reason, entry.inhibited_by, ts))

    lines.append(separator)
    lines.append(f"Total inhibited: {len(ids)}")
    return "\n".join(lines)


def inhibited_summary(inhibitor: PipelineInhibitor) -> str:
    ids = inhibitor.inhibited_pipeline_ids
    if not ids:
        return "All pipelines are active (none inhibited)."
    joined = ", ".join(ids)
    return f"{len(ids)} pipeline(s) inhibited: {joined}"
