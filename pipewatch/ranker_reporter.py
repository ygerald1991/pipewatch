"""Render ranking results as a formatted text table."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_ranker import RankEntry, RankingResult

_HEADER = f"{'Rank':<6} {'Pipeline':<30} {'Score':>8} {'Error Rate':>12} {'Throughput':>12}"
_SEP = "-" * len(_HEADER)


def _format_row(entry: RankEntry) -> str:
    er = f"{entry.error_rate:.4f}" if entry.error_rate is not None else "n/a"
    tp = f"{entry.throughput:.2f}" if entry.throughput is not None else "n/a"
    return (
        f"#{entry.rank:<5} {entry.pipeline_id:<30} "
        f"{entry.composite_score:>8.3f} {er:>12} {tp:>12}"
    )


def render_ranking_table(result: RankingResult, limit: int = 0) -> str:
    """Return a formatted table string for the ranking result.

    Args:
        result: The RankingResult to render.
        limit: If > 0, only show the top *limit* entries.
    """
    entries: List[RankEntry] = result.entries
    if limit > 0:
        entries = entries[:limit]

    if not entries:
        return "No ranking data available."

    lines = [_HEADER, _SEP]
    for entry in entries:
        lines.append(_format_row(entry))
    return "\n".join(lines)


def overall_ranking_status(result: RankingResult) -> str:
    """Return a one-line summary of the worst-ranked pipeline."""
    if not result.entries:
        return "No pipelines ranked."
    worst = result.entries[-1]
    return (
        f"Worst pipeline: {worst.pipeline_id} "
        f"(score={worst.composite_score:.3f}, "
        f"rank #{worst.rank} of {len(result.entries)})"
    )
