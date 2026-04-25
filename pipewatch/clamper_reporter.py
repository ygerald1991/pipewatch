from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_clamper import ClamperResult, ClampEntry


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def _format_row(entry: ClampEntry) -> str:
    changed = (
        entry.original_error_rate != entry.clamped_error_rate
        or entry.original_throughput != entry.clamped_throughput
    )
    flag = "*" if changed else " "
    return (
        f"  {flag} {entry.pipeline_id:<30} "
        f"err: {_fmt(entry.original_error_rate)} -> {_fmt(entry.clamped_error_rate)}  "
        f"tput: {_fmt(entry.original_throughput)} -> {_fmt(entry.clamped_throughput)}"
    )


def render_clamper_table(result: Optional[ClamperResult]) -> str:
    if result is None or not result.entries:
        return "[clamper] No pipelines to display."
    lines = ["[clamper] Pipeline metric clamp report (* = value was clamped):", ""]
    for entry in result.entries:
        lines.append(_format_row(entry))
    lines.append("")
    lines.append(f"  Total pipelines : {len(result.entries)}")
    lines.append(f"  Clamped         : {result.total_clamped}")
    return "\n".join(lines)


def clamped_summary(result: Optional[ClamperResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines processed."
    n = result.total_clamped
    total = len(result.entries)
    return f"{n}/{total} pipeline(s) had metrics clamped."
