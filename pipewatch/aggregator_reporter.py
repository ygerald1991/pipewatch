"""Render aggregate statistics as a human-readable text report."""
from __future__ import annotations

from typing import List

from pipewatch.pipeline_aggregator import AggregateStats


def _fmt(value: object, precision: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{precision}f}"
    return str(value)


def render_aggregate_report(stats: AggregateStats) -> str:
    """Return a multi-line text summary of the aggregate stats."""
    if stats.pipeline_count == 0:
        return "No pipeline data available."

    lines: List[str] = [
        "=== Pipeline Aggregate Summary ===",
        f"  Pipelines monitored : {stats.pipeline_count}",
        f"  Total metrics        : {stats.total_metrics}",
        f"  Degraded pipelines   : {stats.degraded_count}",
        "",
        "  Error Rate",
        f"    avg : {_fmt(stats.avg_error_rate)}",
        f"    max : {_fmt(stats.max_error_rate)}",
        f"    min : {_fmt(stats.min_error_rate)}",
        "",
        "  Throughput (rec/s)",
        f"    avg : {_fmt(stats.avg_throughput, 1)}",
        f"    max : {_fmt(stats.max_throughput, 1)}",
        f"    min : {_fmt(stats.min_throughput, 1)}",
    ]
    return "\n".join(lines)


def overall_aggregate_status(stats: AggregateStats) -> str:
    """Return a single-word status string based on aggregate health."""
    if stats.pipeline_count == 0:
        return "UNKNOWN"
    ratio = stats.degraded_count / stats.pipeline_count
    if ratio == 0:
        return "HEALTHY"
    if ratio <= 0.25:
        return "WARNING"
    return "CRITICAL"
