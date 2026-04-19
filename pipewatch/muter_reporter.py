from __future__ import annotations
from pipewatch.pipeline_muter import MuterResult, MuteEntry


def _format_row(entry: MuteEntry) -> str:
    status = "ACTIVE" if entry.is_active() else "EXPIRED"
    expires = entry.expires_at().strftime("%Y-%m-%d %H:%M:%S")
    reason = entry.reason or "-"
    return f"  {entry.pipeline_id:<30} {status:<10} {expires:<22} {reason}"


def render_muter_table(result: MuterResult) -> str:
    if not result.entries:
        return "No muted pipelines."

    header = f"  {'Pipeline ID':<30} {'Status':<10} {'Expires At':<22} Reason"
    separator = "  " + "-" * 78
    rows = [_format_row(e) for e in result.entries]
    active = result.total_muted
    total = len(result.entries)
    footer = f"\n  Muted: {active} active / {total} total"
    return "\n".join([header, separator] + rows) + footer


def muted_summary(result: MuterResult) -> str:
    active = result.total_muted
    if active == 0:
        return "All pipelines are unmuted."
    return f"{active} pipeline(s) currently muted."
