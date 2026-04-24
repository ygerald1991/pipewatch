from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_escalator import EscalatorResult

_LEVEL_LABELS = {
    0: "none",
    1: "low",
    2: "medium",
    3: "high",
}


def _format_row(pipeline_id: str, level: int, reason: str) -> str:
    label = _LEVEL_LABELS.get(level, f"level-{level}")
    reason_str = reason if reason else "-"
    return f"  {pipeline_id:<30} {label:<10} {reason_str}"


def render_escalator_table(result: Optional[EscalatorResult]) -> str:
    if result is None or not result.entries:
        return "No escalation data."

    lines = [
        f"  {'Pipeline':<30} {'Level':<10} Reason",
        "  " + "-" * 60,
    ]
    for entry in result.entries:
        lines.append(_format_row(entry.pipeline_id, entry.level, entry.reason))
    return "\n".join(lines)


def escalated_summary(result: Optional[EscalatorResult]) -> str:
    if result is None:
        return "No escalation result available."

    total = result.total_escalated
    if total == 0:
        return "All pipelines are at baseline escalation level."

    level_counts: dict[int, int] = {}
    for entry in result.entries:
        if entry.level > 0:
            level_counts[entry.level] = level_counts.get(entry.level, 0) + 1

    parts = [
        f"level-{lvl}={count}"
        for lvl, count in sorted(level_counts.items())
    ]
    return f"{total} pipeline(s) escalated: {', '.join(parts)}"
