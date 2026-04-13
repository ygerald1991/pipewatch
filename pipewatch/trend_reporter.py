"""Render trend analysis results as human-readable text."""

from typing import Dict, List
from pipewatch.trend_analyzer import TrendResult

_DIRECTION_ICON = {
    "improving": "\u2197",   # ↗
    "degrading": "\u2198",   # ↘
    "stable": "\u2192",      # →
}


def _format_trend_row(result: TrendResult) -> str:
    icon = _DIRECTION_ICON.get(result.direction, "?")
    return (
        f"  {icon}  {result.metric_name:<16} "
        f"{result.direction:<12} "
        f"delta={result.delta:+.4f}  "
        f"(n={result.sample_count})"
    )


def render_trend_table(
    trend_data: Dict[str, List[TrendResult]],
) -> str:
    """Render a full trend report for all pipelines."""
    if not trend_data:
        return "No trend data available."

    lines: List[str] = []
    lines.append("=" * 60)
    lines.append("  PIPELINE TREND REPORT")
    lines.append("=" * 60)

    for pipeline_id, results in sorted(trend_data.items()):
        lines.append(f"\nPipeline: {pipeline_id}")
        lines.append("-" * 40)
        if not results:
            lines.append("  (insufficient data for trend analysis)")
        else:
            for r in results:
                lines.append(_format_trend_row(r))

    lines.append("")
    return "\n".join(lines)


def overall_trend_status(trend_data: Dict[str, List[TrendResult]]) -> str:
    """Return 'ok', 'warning', or 'critical' based on degrading trends."""
    degrading_count = sum(
        1
        for results in trend_data.values()
        for r in results
        if r.direction == "degrading"
    )
    if degrading_count == 0:
        return "ok"
    if degrading_count <= 2:
        return "warning"
    return "critical"
