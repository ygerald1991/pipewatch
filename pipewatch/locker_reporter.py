from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_locker import LockerResult


def _format_row(pipeline_id: str, locked_by: str, reason: str, status: str) -> str:
    return f"  {pipeline_id:<30} {locked_by:<20} {reason:<30} {status}"


def render_locker_table(result: Optional[LockerResult]) -> str:
    if result is None or not result.snapshots:
        return "No pipelines to display."

    locked_ids = set(result.pipeline_ids)
    lines = [
        f"{'Pipeline':<30} {'Locked By':<20} {'Reason':<30} {'Status'}",
        "-" * 90,
    ]
    lock_map = {e.pipeline_id: e for e in result.entries}

    for snap in result.snapshots:
        if snap.pipeline_id in lock_map:
            e = lock_map[snap.pipeline_id]
            lines.append(_format_row(snap.pipeline_id, e.locked_by, e.reason, "LOCKED"))
        else:
            lines.append(_format_row(snap.pipeline_id, "-", "-", "unlocked"))

    return "\n".join(lines)


def locked_summary(result: Optional[LockerResult]) -> str:
    if result is None:
        return "No locker result available."
    return (
        f"{result.total_locked} locked, {result.total_unlocked} unlocked "
        f"out of {len(result.snapshots)} pipelines."
    )
