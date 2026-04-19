from typing import Optional
from pipewatch.pipeline_cooldown import CooldownResult


def _format_row(pipeline_id: str, expires: str, reason: str, active: bool) -> str:
    status = "COOLING" if active else "EXPIRED"
    return f"  {pipeline_id:<30} {status:<10} {expires:<26} {reason}"


def render_cooldown_table(result: Optional[CooldownResult]) -> str:
    if result is None or not result.entries:
        return "No cooldown entries."

    lines = [
        "Pipeline Cooldown Status",
        "-" * 80,
        f"  {'Pipeline':<30} {'Status':<10} {'Expires':<26} Reason",
        "-" * 80,
    ]
    for entry in result.entries:
        lines.append(
            _format_row(
                entry.pipeline_id,
                entry.expires_at().isoformat(),
                entry.reason or "-",
                entry.is_active(),
            )
        )
    lines.append("-" * 80)
    lines.append(f"  Cooling: {result.total_cooling}  Allowed through: {len(result.snapshots)}")
    return "\n".join(lines)


def cooling_summary(result: Optional[CooldownResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines in cooldown."
    ids = ", ".join(e.pipeline_id for e in result.entries if e.is_active())
    return f"{result.total_cooling} pipeline(s) cooling: {ids}" if ids else "No active cooldowns."
