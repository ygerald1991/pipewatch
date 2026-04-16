from __future__ import annotations
from typing import List
from pipewatch.pipeline_tracer import TraceResult


def _format_row(result: TraceResult) -> str:
    status = "INCOMPLETE" if result.has_incomplete_spans else "COMPLETE"
    return (
        f"  {result.pipeline_id:<30} "
        f"stages={result.stage_count:<5} "
        f"total={result.total_duration:.2f}s "
        f"status={status}"
    )


def render_trace_table(results: List[TraceResult]) -> str:
    if not results:
        return "No trace data available."
    lines = [
        "Pipeline Trace Report",
        "=" * 70,
        f"  {'Pipeline':<30} {'Stages':<12} {'Duration':<14} Status",
        "-" * 70,
    ]
    for r in results:
        lines.append(_format_row(r))
    lines.append("=" * 70)
    return "\n".join(lines)


def incomplete_traces_summary(results: List[TraceResult]) -> str:
    incomplete = [r for r in results if r.has_incomplete_spans]
    if not incomplete:
        return "All traces complete."
    ids = ", ".join(r.pipeline_id for r in incomplete)
    return f"{len(incomplete)} incomplete trace(s): {ids}"
