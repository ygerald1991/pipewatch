"""Render comparison results as a formatted text table."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_comparator import ComparisonResult

_HEADER = f"{'Pipeline':<24} {'Metric':<14} {'Value':>10} {'Mean':>10} {'Delta':>10} {'Outlier':>8}"
_SEP = "-" * len(_HEADER)


def _format_row(result: ComparisonResult) -> str:
    flag = "YES" if result.is_outlier else "-"
    return (
        f"{result.pipeline_id:<24} "
        f"{result.metric:<14} "
        f"{result.value:>10.4f} "
        f"{result.mean:>10.4f} "
        f"{result.delta:>+10.4f} "
        f"{flag:>8}"
    )


def render_comparison_table(results: List[ComparisonResult]) -> str:
    """Return a formatted string table of comparison results."""
    if not results:
        return "No comparison data available."

    lines = [_HEADER, _SEP]
    for r in sorted(results, key=lambda x: (x.metric, x.pipeline_id)):
        lines.append(_format_row(r))
    return "\n".join(lines)


def outlier_summary(results: List[ComparisonResult]) -> str:
    """Return a short human-readable summary of outlier pipelines."""
    outliers = [r for r in results if r.is_outlier]
    if not outliers:
        return "All pipelines are within normal range."

    parts = []
    for r in outliers:
        direction = "high" if r.delta > 0 else "low"
        parts.append(f"{r.pipeline_id} ({r.metric} {direction})")
    return "Outliers detected: " + ", ".join(parts)
