from __future__ import annotations
from pipewatch.pipeline_fencer import FencerResult, PipelineFencer


def _format_row(pipeline_id: str, status: str) -> str:
    return f"  {pipeline_id:<30} {status}"


def render_fencer_table(result: FencerResult) -> str:
    if result.total_fenced == 0 and result.total_allowed == 0:
        return "No pipelines evaluated."

    lines = ["Pipeline Fence Status", "-" * 45]
    for pid in result.fenced:
        lines.append(_format_row(pid, "FENCED"))
    for pid in result.allowed:
        lines.append(_format_row(pid, "allowed"))
    lines.append("-" * 45)
    lines.append(f"  Fenced: {result.total_fenced}  Allowed: {result.total_allowed}")
    return "\n".join(lines)


def fenced_summary(fencer: PipelineFencer) -> str:
    active = fencer.fenced_pipelines()
    if not active:
        return "No pipelines currently fenced."
    ids = ", ".join(active)
    return f"Fenced pipelines ({len(active)}): {ids}"
