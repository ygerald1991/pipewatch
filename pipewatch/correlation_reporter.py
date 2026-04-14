"""Render correlation results as a human-readable table."""

from __future__ import annotations

from typing import List

from pipewatch.correlation import CorrelationResult

_HEADER = f"{'Pipeline A':<20} {'Pipeline B':<20} {'Metric':<14} {'r':>8}  Sig"
_DIVIDER = "-" * 70


def _format_row(result: CorrelationResult) -> str:
    sig = "YES" if result.is_significant else "no"
    return (
        f"{result.pipeline_a:<20} {result.pipeline_b:<20} "
        f"{result.metric:<14} {result.coefficient:>8.3f}  {sig}"
    )


def render_correlation_table(results: List[CorrelationResult]) -> str:
    if not results:
        return "No correlation data available."
    lines = [_HEADER, _DIVIDER]
    for r in sorted(results, key=lambda x: abs(x.coefficient), reverse=True):
        lines.append(_format_row(r))
    return "\n".join(lines)


def significant_pairs_summary(results: List[CorrelationResult]) -> str:
    """Return a short summary sentence about significant correlations."""
    significant = [r for r in results if r.is_significant]
    if not significant:
        return "No significant cross-pipeline correlations detected."
    pairs = ", ".join(
        f"{r.pipeline_a}/{r.pipeline_b} ({r.metric})"
        for r in significant
    )
    return f"{len(significant)} significant correlation(s) found: {pairs}."
