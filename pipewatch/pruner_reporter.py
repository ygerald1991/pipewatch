from __future__ import annotations
from pipewatch.pipeline_pruner import PrunerResult, PruneResult


def _format_row(result: PruneResult) -> str:
    return (
        f"  {result.pipeline_id:<30} "
        f"original={result.original_count:<6} "
        f"pruned={result.pruned_count:<6} "
        f"kept={result.kept_count}"
    )


def render_pruner_table(result: PrunerResult) -> str:
    if not result.results:
        return "No pipelines pruned."

    lines = [
        "Pipeline Pruner Report",
        "=" * 60,
    ]
    for r in result.results:
        lines.append(_format_row(r))

    lines.append("-" * 60)
    lines.append(
        f"  Total pruned: {result.total_pruned}  |  Total kept: {result.total_kept}"
    )
    return "\n".join(lines)


def pruner_summary(result: PrunerResult) -> str:
    if result.total_pruned == 0:
        return "All snapshots within retention limits."
    return (
        f"{result.total_pruned} snapshot(s) pruned across "
        f"{len(result.results)} pipeline(s)."
    )
