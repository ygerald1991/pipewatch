from __future__ import annotations
from typing import List
from pipewatch.pipeline_stager import StagerResult, StageResult


def _format_row(r: StageResult) -> str:
    er = f"{r.avg_error_rate:.3f}" if r.avg_error_rate is not None else "n/a"
    tp = f"{r.avg_throughput:.1f}" if r.avg_throughput is not None else "n/a"
    return f"  {r.stage:<16} {r.pipeline_id:<24} {er:<12} {tp:<12} {r.snapshot_count}"


def render_stager_table(result: StagerResult) -> str:
    if not result.stages:
        return "No staged pipelines."

    header = f"  {'Stage':<16} {'Pipeline':<24} {'Avg ErrRate':<12} {'Avg Throughput':<12} Snapshots"
    separator = "  " + "-" * 78
    rows = [_format_row(r) for r in result.stages]
    return "\n".join([header, separator] + rows)


def stage_summary(result: StagerResult) -> str:
    if not result.stages:
        return "No stages recorded."

    stage_names = sorted({r.stage for r in result.stages})
    lines = []
    for stage in stage_names:
        entries = result.for_stage(stage)
        lines.append(f"  {stage}: {len(entries)} pipeline(s)")
    return "Stage summary:\n" + "\n".join(lines)
