"""Render pipeline scoring results as a formatted text table."""

from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_scorer import ScoringResult

_HEADER = f"{'Rank':<6}{'Pipeline':<30}{'Score':>7}{'Grade':>7}{'Error Rate':>12}{'Throughput':>12}"
_SEP = "-" * len(_HEADER)


def _format_row(rank: int, pipeline_id: str, score: float, grade: str,
                error_rate: Optional[float], throughput: Optional[float]) -> str:
    er = f"{error_rate:.3f}" if error_rate is not None else "n/a"
    tp = f"{throughput:.1f}" if throughput is not None else "n/a"
    return f"{rank:<6}{pipeline_id:<30}{score:>7.1f}{grade:>7}{er:>12}{tp:>12}"


def render_scoring_table(result: ScoringResult) -> str:
    """Return a human-readable ranking table."""
    if not result.scores:
        return "No pipeline scores available."

    lines = [_HEADER, _SEP]
    for ps in result.scores:
        row = _format_row(
            rank=ps.rank,
            pipeline_id=ps.pipeline_id,
            score=ps.health.score,
            grade=ps.health.grade,
            error_rate=ps.health.error_rate,
            throughput=ps.health.throughput,
        )
        lines.append(row)

    return "\n".join(lines)


def overall_scoring_status(result: ScoringResult) -> str:
    """Return a one-line summary of the scoring session."""
    if not result.scores:
        return "No pipelines scored."

    total = len(result.scores)
    degraded = sum(1 for s in result.scores if s.health.score < 60.0)
    if degraded == 0:
        return f"All {total} pipeline(s) healthy."
    return f"{degraded}/{total} pipeline(s) degraded (score < 60)."
