from __future__ import annotations

from typing import List

from pipewatch.pipeline_scheduler import PipelineScheduler, SchedulerResult


def _format_row(pipeline_id: str, interval: int, last_run: str, status: str) -> str:
    return f"  {pipeline_id:<30} {interval:>10}s   {last_run:<24}  {status}"


def render_scheduler_table(scheduler: PipelineScheduler) -> str:
    ids = scheduler.pipeline_ids()
    if not ids:
        return "No pipelines registered in scheduler."

    lines: List[str] = [
        "Pipeline Scheduler",
        "-" * 72,
        _format_row("Pipeline", 0, "Last Run", "Status").replace("         0s", "  Interval"),
        "-" * 72,
    ]
    # Re-render header cleanly
    lines[2] = f"  {'Pipeline':<30} {'Interval':>10}    {'Last Run':<24}  Status"

    for pid in sorted(ids):
        entry = scheduler.entry_for(pid)
        if entry is None:
            continue
        last = entry.last_run.isoformat() if entry.last_run else "never"
        status = "enabled" if entry.enabled else "disabled"
        lines.append(_format_row(pid, entry.interval_seconds, last, status))

    lines.append("-" * 72)
    return "\n".join(lines)


def render_tick_summary(result: SchedulerResult) -> str:
    lines = [
        "Scheduler Tick Summary",
        f"  Triggered : {len(result.triggered)}",
        f"  Skipped   : {len(result.skipped)}",
        f"  Total     : {result.total}",
    ]
    if result.triggered:
        lines.append("  Triggered pipelines: " + ", ".join(sorted(result.triggered)))
    return "\n".join(lines)


def overall_scheduler_status(result: SchedulerResult) -> str:
    if not result.triggered:
        return "idle"
    return "active"
