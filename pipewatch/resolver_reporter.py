from __future__ import annotations
from typing import Optional
from pipewatch.pipeline_resolver import ResolverResult


def _format_row(pipeline_id: str, resolved_id: str, alias: Optional[str], remapped: bool) -> str:
    alias_col = alias if alias else "-"
    status = "remapped" if remapped else "unchanged"
    return f"  {pipeline_id:<24} {resolved_id:<24} {alias_col:<20} {status}"


def render_resolver_table(result: Optional[ResolverResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines to resolve."

    header = f"  {'Pipeline ID':<24} {'Resolved ID':<24} {'Alias':<20} Status"
    divider = "  " + "-" * 76
    rows = [
        _format_row(e.pipeline_id, e.resolved_id, e.alias, e.was_remapped)
        for e in result.entries
    ]
    lines = ["Pipeline Resolver", divider, header, divider] + rows + [divider]
    lines.append(
        f"  Total: {len(result.entries)}  "
        f"Remapped: {result.total_remapped}  "
        f"Unchanged: {result.total_unchanged}"
    )
    return "\n".join(lines)


def remapped_summary(result: Optional[ResolverResult]) -> str:
    if result is None or not result.entries:
        return "No resolver data available."
    remapped = [e for e in result.entries if e.was_remapped]
    if not remapped:
        return "All pipeline IDs resolved without remapping."
    names = ", ".join(e.pipeline_id for e in remapped)
    return f"{len(remapped)} pipeline(s) remapped: {names}"
