from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_bumper import BumperResult, BumpEntry


def _format_row(entry: BumpEntry) -> str:
    return (
        f"  {entry.pipeline_id:<30} "
        f"{entry.previous_priority:>4} -> {entry.new_priority:<4} "
        f"  {entry.reason}"
    )


def render_bumper_table(result: Optional[BumperResult]) -> str:
    if result is None or result.total_bumped == 0:
        return "No pipelines bumped."

    header = f"  {'PIPELINE':<30} {'PREV':>4}    {'NEW':<4}   REASON"
    separator = "-" * 60
    rows = [_format_row(e) for e in result.entries]
    return "\n".join([header, separator] + rows)


def bumped_summary(result: Optional[BumperResult]) -> str:
    if result is None or result.total_bumped == 0:
        return "No pipelines were priority-bumped."
    ids = ", ".join(result.pipeline_ids)
    return f"{result.total_bumped} pipeline(s) bumped: {ids}"
