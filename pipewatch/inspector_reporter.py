from pipewatch.pipeline_inspector import InspectionResult


def _format_row(result: InspectionResult) -> str:
    err = f"{result.avg_error_rate:.3f}" if result.avg_error_rate is not None else "n/a"
    thr = f"{result.avg_throughput:.1f}" if result.avg_throughput is not None else "n/a"
    flags = ", ".join(result.flags) if result.flags else "-"
    return f"  {result.pipeline_id:<24} {result.snapshot_count:>6}  {err:>10}  {thr:>12}  {flags}"


def render_inspection_table(results: list) -> str:
    if not results:
        return "No inspection data available."

    header = f"  {'Pipeline':<24} {'Snaps':>6}  {'Avg Err':>10}  {'Avg Tput':>12}  Flags"
    divider = "  " + "-" * 70
    rows = [_format_row(r) for r in results]
    return "\n".join([header, divider] + rows)


def flagged_summary(results: list) -> str:
    flagged = [r for r in results if r.flags]
    if not flagged:
        return "All pipelines passed inspection."
    lines = [f"  {r.pipeline_id}: {', '.join(r.flags)}" for r in flagged]
    return f"{len(flagged)} pipeline(s) flagged:\n" + "\n".join(lines)
