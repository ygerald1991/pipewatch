from __future__ import annotations
from pipewatch.pipeline_dispatcher import DispatcherResult


def _format_row(pipeline_id: str, target: str, rule_name: str) -> str:
    return f"  {pipeline_id:<30} {target:<20} {rule_name}"


def render_dispatcher_table(result: DispatcherResult) -> str:
    if not result.results:
        return "No dispatch results."
    lines = ["Pipeline Dispatch Results", "=" * 60]
    lines.append(_format_row("Pipeline ID", "Target", "Rule"))
    lines.append("-" * 60)
    for r in result.results:
        target = r.target or "(unmatched)"
        rule = r.rule_name or "-"
        lines.append(_format_row(r.pipeline_id, target, rule))
    return "\n".join(lines)


def target_summary(result: DispatcherResult) -> str:
    targets = result.targets()
    if not targets:
        return "No matched targets."
    lines = ["Target Summary"]
    for target, pids in sorted(targets.items()):
        lines.append(f"  {target}: {', '.join(pids)}")
    unmatched = result.unmatched()
    if unmatched:
        lines.append(f"  (unmatched): {', '.join(r.pipeline_id for r in unmatched)}")
    return "\n".join(lines)
