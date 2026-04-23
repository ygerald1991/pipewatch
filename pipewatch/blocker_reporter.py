from __future__ import annotations
from pipewatch.pipeline_blocker import BlockerResult


def _format_row(pipeline_id: str, status: str, reason: str) -> str:
    return f"  {pipeline_id:<30} {status:<10} {reason}"


def render_blocker_table(result: BlockerResult) -> str:
    """Render a formatted table report of pipeline blocker results.

    Args:
        result: A BlockerResult containing blocked and allowed pipeline snapshots.

    Returns:
        A multi-line string table suitable for console output.
    """
    if not result.pipeline_ids:
        return "No pipelines tracked by blocker."

    lines = ["Pipeline Blocker Report", "=" * 55]
    header = _format_row("PIPELINE", "STATUS", "REASON")
    lines.append(header)
    lines.append("-" * 55)

    for entry in result.blocked:
        lines.append(_format_row(entry.pipeline_id, "BLOCKED", entry.reason))

    for snapshot in result.allowed:
        lines.append(_format_row(snapshot.pipeline_id, "ALLOWED", "-"))

    lines.append("-" * 55)
    lines.append(f"  Blocked: {result.total_blocked}  Allowed: {result.total_allowed}")
    return "\n".join(lines)


def blocked_summary(result: BlockerResult) -> str:
    """Return a brief human-readable summary of blocked pipelines.

    Args:
        result: A BlockerResult containing blocked and allowed pipeline snapshots.

    Returns:
        A single-line summary string.
    """
    if result.total_blocked == 0:
        return "All pipelines are allowed."
    ids = ", ".join(e.pipeline_id for e in result.blocked)
    return f"{result.total_blocked} pipeline(s) blocked: {ids}"
