from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_balancer import BalancerResult, BalanceEntry


def _format_row(entry: BalanceEntry) -> str:
    state = "OVERLOADED" if entry.overloaded else "OK"
    return (
        f"  {entry.pipeline_id:<30} "
        f"slot={entry.assigned_slot}  "
        f"load={entry.load_score:.3f}  "
        f"[{state}]"
    )


def render_balancer_table(result: Optional[BalancerResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines to balance."

    lines = ["Pipeline Balancer", "-" * 60]
    for entry in sorted(result.entries, key=lambda e: e.assigned_slot):
        lines.append(_format_row(entry))
    lines.append("-" * 60)
    lines.append(
        f"Balanced: {result.total_balanced}  "
        f"Overloaded: {result.total_overloaded}"
    )
    return "\n".join(lines)


def overloaded_summary(result: Optional[BalancerResult]) -> str:
    if result is None or not result.entries:
        return "No balance data available."

    overloaded = [e for e in result.entries if e.overloaded]
    if not overloaded:
        return "All pipelines are within load limits."

    ids = ", ".join(e.pipeline_id for e in overloaded)
    return f"{len(overloaded)} overloaded pipeline(s): {ids}"
