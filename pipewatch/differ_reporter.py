"""Renders diff results as a human-readable table."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_differ import DiffResult

_COL_PIPELINE = 20
_COL_FIELD = 14
_COL_BEFORE = 12
_COL_AFTER = 12
_COL_DELTA = 12
_COL_PCT = 10


def _fmt(value: object, width: int) -> str:
    s = "N/A" if value is None else f"{value:.4f}" if isinstance(value, float) else str(value)
    return s.ljust(width)


def _format_row(pipeline_id: str, field_name: str, before: object, after: object,
               delta: object, pct: object) -> str:
    return (
        _fmt(pipeline_id, _COL_PIPELINE)
        + _fmt(field_name, _COL_FIELD)
        + _fmt(before, _COL_BEFORE)
        + _fmt(after, _COL_AFTER)
        + _fmt(delta, _COL_DELTA)
        + _fmt(pct, _COL_PCT)
    )


def render_diff_table(results: List[DiffResult]) -> str:
    if not results:
        return "No diff data available."

    header = _format_row("PIPELINE", "FIELD", "BEFORE", "AFTER", "DELTA", "PCT CHANGE")
    separator = "-" * (_COL_PIPELINE + _COL_FIELD + _COL_BEFORE + _COL_AFTER + _COL_DELTA + _COL_PCT)
    rows = [header, separator]

    for result in results:
        for diff in result.diffs:
            pct_str = None if diff.pct_change is None else f"{diff.pct_change:+.1f}%"
            rows.append(
                _format_row(
                    result.pipeline_id,
                    diff.field_name,
                    diff.before,
                    diff.after,
                    diff.delta,
                    pct_str,
                )
            )

    return "\n".join(rows)


def changed_fields_summary(results: List[DiffResult]) -> str:
    changed = [r for r in results if r.has_changes]
    if not changed:
        return "No field-level changes detected."
    lines = [f"Changed pipelines ({len(changed)}):"]
    for r in changed:
        lines.append(f"  {r.pipeline_id}: {', '.join(r.changed_fields)}")
    return "\n".join(lines)
