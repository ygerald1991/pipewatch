from pipewatch.pipeline_whitelister import WhitelisterResult


def _format_row(pipeline_id: str, allowed: bool) -> str:
    status = "ALLOWED" if allowed else "BLOCKED"
    return f"  {pipeline_id:<30} {status}"


def render_whitelister_table(result: WhitelisterResult) -> str:
    if not result.entries:
        return "No pipelines evaluated."

    lines = ["Pipeline Whitelist Report", "-" * 45]
    for snapshot, allowed in result.entries:
        lines.append(_format_row(snapshot.pipeline_id, allowed))
    lines.append("-" * 45)
    lines.append(f"Allowed: {result.total_allowed}  Blocked: {result.total_blocked}")
    return "\n".join(lines)


def whitelisted_summary(result: WhitelisterResult) -> str:
    ids = [s.pipeline_id for s, allowed in result.entries if allowed]
    if not ids:
        return "No pipelines are whitelisted."
    return "Whitelisted: " + ", ".join(ids)
