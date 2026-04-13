"""Renders a health score table for display in the CLI."""

from typing import List

from pipewatch.health_score import HealthScore

_HEADER = f"{'Pipeline':<30} {'Score':>7} {'Grade':>6} {'Error Rate':>12} {'Throughput':>12}"
_DIVIDER = "-" * len(_HEADER)


def _format_row(hs: HealthScore) -> str:
    return (
        f"{hs.pipeline_id:<30} "
        f"{hs.score:>7.1f} "
        f"{hs.grade:>6} "
        f"{hs.error_rate * 100:>11.1f}% "
        f"{hs.throughput:>11.2f}/s"
    )


def render_health_table(scores: List[HealthScore]) -> str:
    """Render a formatted health score table.

    Args:
        scores: List of HealthScore objects to display.

    Returns:
        A multi-line string suitable for CLI output.
    """
    if not scores:
        return "No health data available."

    lines = [_HEADER, _DIVIDER]
    for hs in sorted(scores, key=lambda h: h.score):
        lines.append(_format_row(hs))
    lines.append(_DIVIDER)

    avg_score = sum(h.score for h in scores) / len(scores)
    lines.append(f"{'Average':<30} {avg_score:>7.1f}")
    return "\n".join(lines)


def overall_health_status(scores: List[HealthScore]) -> str:
    """Return a single-line summary of overall pipeline health.

    Args:
        scores: List of HealthScore objects.

    Returns:
        A summary string indicating the worst health grade observed.
    """
    if not scores:
        return "UNKNOWN — no pipelines tracked"

    worst = min(scores, key=lambda h: h.score)
    if worst.score >= 90:
        return "HEALTHY — all pipelines operating normally"
    elif worst.score >= 60:
        return f"DEGRADED — worst pipeline: {worst.pipeline_id} ({worst.grade})"
    else:
        return f"CRITICAL — pipeline {worst.pipeline_id} health score {worst.score:.1f}/100"
