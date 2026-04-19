from pipewatch.pipeline_resetter import ResetterResult


def _format_row(pipeline_id: str, reason: str, prev_error: str, prev_throughput: str) -> str:
    return f"  {pipeline_id:<30} {reason:<20} {prev_error:<15} {prev_throughput}"


def render_resetter_table(result: ResetterResult) -> str:
    if not result.entries:
        return "No pipelines were reset."

    lines = [
        "Pipeline Resetter Report",
        "=" * 80,
        _format_row("Pipeline ID", "Reason", "Prev Error Rate", "Prev Throughput"),
        "-" * 80,
    ]
    for entry in result.entries:
        prev_err = f"{entry.previous_error_rate:.4f}" if entry.previous_error_rate is not None else "N/A"
        prev_thr = f"{entry.previous_throughput:.2f}" if entry.previous_throughput is not None else "N/A"
        lines.append(_format_row(entry.pipeline_id, entry.reason, prev_err, prev_thr))

    lines.append("-" * 80)
    lines.append(f"Total reset: {result.total_reset}")
    return "\n".join(lines)


def reset_summary(result: ResetterResult) -> str:
    if result.total_reset == 0:
        return "No pipelines reset."
    ids = ", ".join(result.pipeline_ids)
    return f"{result.total_reset} pipeline(s) reset: {ids}"
