from __future__ import annotations
from pipewatch.pipeline_limiter import LimiterResult, LimitResult


def _format_row(r: LimitResult) -> str:
    status = "LIMITED" if r.limited else "OK"
    return f"  {r.pipeline_id:<30} {status:<10} {r.snapshot_count:>6}/{r.limit:<6} dropped={r.dropped}"


def render_limiter_table(result: LimiterResult) -> str:
    if not result.results:
        return "No pipelines to report."
    header = f"  {'Pipeline':<30} {'Status':<10} {'Count/Limit':<14} Dropped"
    divider = "  " + "-" * 60
    rows = [_format_row(r) for r in result.results]
    return "\n".join([header, divider] + rows)


def limited_summary(result: LimiterResult) -> str:
    limited = result.limited_pipelines()
    total_dropped = result.total_dropped()
    if not limited:
        return f"All {len(result.results)} pipeline(s) within limits."
    ids = ", ".join(r.pipeline_id for r in limited)
    return (
        f"{len(limited)} pipeline(s) limited: {ids} "
        f"(total dropped={total_dropped})"
    )
