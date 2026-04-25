from __future__ import annotations
from pipewatch.pipeline_gatekeeper import GatekeeperResult, GateEntry
from typing import Optional


def _format_row(entry: GateEntry) -> str:
    status = "ALLOWED" if entry.allowed else "BLOCKED"
    return f"  {entry.pipeline_id:<30} {status:<10} {entry.reason}"


def render_gatekeeper_table(result: Optional[GatekeeperResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines evaluated."

    lines = [
        f"{'Pipeline':<30} {'Status':<10} Reason",
        "-" * 70,
    ]
    for entry in sorted(result.entries, key=lambda e: e.pipeline_id):
        lines.append(_format_row(entry))

    lines.append("-" * 70)
    lines.append(
        f"Total: {result.total_allowed} allowed, {result.total_blocked} blocked"
    )
    return "\n".join(lines)


def blocked_summary(result: Optional[GatekeeperResult]) -> str:
    if result is None or result.total_blocked == 0:
        return "All pipelines passed the gate."
    ids = ", ".join(result.blocked_pipelines())
    return f"{result.total_blocked} pipeline(s) blocked: {ids}"
