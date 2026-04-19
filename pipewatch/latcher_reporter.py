from pipewatch.pipeline_latcher import LatcherResult


def _format_row(pipeline_id: str, reason: str, latched_by: str, latched_at: str) -> str:
    return f"  {pipeline_id:<24} {reason:<28} {latched_by:<14} {latched_at}"


def render_latcher_table(result: LatcherResult) -> str:
    if not result.entries:
        return "No latched pipelines."

    header = _format_row("PIPELINE", "REASON", "LATCHED BY", "LATCHED AT")
    divider = "-" * len(header)
    rows = [
        _format_row(
            e.pipeline_id,
            e.reason,
            e.latched_by,
            e.latched_at.isoformat(),
        )
        for e in result.entries
    ]
    return "\n".join([header, divider] + rows)


def latched_summary(result: LatcherResult) -> str:
    total = result.total_latched
    if total == 0:
        return "All pipelines unlatched."
    ids = ", ".join(result.pipeline_ids)
    return f"{total} latched pipeline(s): {ids}"
