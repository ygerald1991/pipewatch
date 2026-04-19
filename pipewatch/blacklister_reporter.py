from __future__ import annotations
from pipewatch.pipeline_blacklister import BlacklisterResult


def _format_row(pipeline_id: str, status: str, reason: str) -> str:
    return f"  {pipeline_id:<30} {status:<12} {reason}"


def render_blacklister_table(result: BlacklisterResult) -> str:
    if result.total_blocked == 0 and result.total_allowed == 0:
        return "No pipelines processed."

    lines = ["Pipeline Blacklist Report", "=" * 60]
    header = _format_row("PIPELINE", "STATUS", "REASON")
    lines.append(header)
    lines.append("-" * 60)

    for entry in result.entries:
        lines.append(_format_row(entry.pipeline_id, "BLOCKED", entry.reason or "-"))

    for snap in result.allowed:
        lines.append(_format_row(snap.pipeline_id, "ALLOWED", "-"))

    lines.append("-" * 60)
    lines.append(f"Blocked: {result.total_blocked}  Allowed: {result.total_allowed}")
    return "\n".join(lines)


def blacklisted_summary(result: BlacklisterResult) -> str:
    if result.total_blocked == 0:
        return "No pipelines are blacklisted."
    ids = ", ".join(result.pipeline_ids)
    return f"{result.total_blocked} blacklisted pipeline(s): {ids}"
