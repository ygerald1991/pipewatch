from __future__ import annotations
from pipewatch.pipeline_pinner import PinnerResult, PipelinePinner


def _format_row(pipeline_id: str, status: str) -> str:
    return f"  {pipeline_id:<30} {status}"


def render_pinner_table(result: PinnerResult) -> str:
    if not result.pinned and not result.unpinned:
        return "No pipelines found."

    lines = ["Pipeline Pins", "-" * 45]
    for pid in result.pinned:
        lines.append(_format_row(pid, "PINNED"))
    for pid in result.unpinned:
        lines.append(_format_row(pid, "unpinned"))
    lines.append("-" * 45)
    lines.append(f"Pinned: {result.total_pinned}  Unpinned: {result.total_unpinned}")
    return "\n".join(lines)


def pinned_summary(pinner: PipelinePinner) -> str:
    ids = pinner.pinned_ids()
    if not ids:
        return "No pipelines are currently pinned."
    joined = ", ".join(ids)
    return f"Pinned pipelines ({len(ids)}): {joined}"
