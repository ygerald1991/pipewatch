from __future__ import annotations
from pipewatch.pipeline_activator import ActivatorResult


def _format_row(pipeline_id: str, activated_by: str, activated_at: str) -> str:
    return f"  {pipeline_id:<30} {activated_by:<20} {activated_at}"


def render_activator_table(result: ActivatorResult) -> str:
    if not result.entries:
        return "No active pipelines."

    header = _format_row("PIPELINE", "ACTIVATED BY", "ACTIVATED AT")
    separator = "  " + "-" * 70
    rows = [
        _format_row(
            e.pipeline_id,
            e.activated_by,
            e.activated_at.isoformat(),
        )
        for e in result.entries
    ]
    return "\n".join([header, separator] + rows)


def active_summary(result: ActivatorResult) -> str:
    total = result.total_active
    if total == 0:
        return "No pipelines are currently active."
    ids = ", ".join(result.pipeline_ids)
    return f"{total} active pipeline(s): {ids}"
