from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_diverter import DiverterResult


def _format_row(pipeline_id: str, destination: str, diverted: bool, reason: str) -> str:
    status = "DIVERTED" if diverted else "UNCHANGED"
    return f"  {pipeline_id:<30} {destination:<20} {status:<12} {reason}"


def render_diverter_table(result: Optional[DiverterResult]) -> str:
    if result is None or not result.entries:
        return "No pipelines to divert."

    header = f"  {'Pipeline':<30} {'Destination':<20} {'Status':<12} Reason"
    separator = "  " + "-" * 80
    lines = ["Pipeline Diverter Report", separator, header, separator]

    for entry in result.entries:
        diverted = entry.diverted_to != entry.original_destination
        lines.append(
            _format_row(entry.pipeline_id, entry.diverted_to, diverted, entry.reason)
        )

    lines.append(separator)
    lines.append(
        f"  Total: {len(result.entries)} pipelines | "
        f"Diverted: {result.total_diverted} | "
        f"Unchanged: {result.total_unchanged}"
    )
    return "\n".join(lines)


def diverted_destinations_summary(result: Optional[DiverterResult]) -> str:
    if result is None or not result.entries:
        return "No diversion data available."

    dest_counts: dict[str, int] = {}
    for entry in result.entries:
        dest_counts[entry.diverted_to] = dest_counts.get(entry.diverted_to, 0) + 1

    lines = ["Destination Summary:"]
    for dest, count in sorted(dest_counts.items()):
        lines.append(f"  {dest}: {count} pipeline(s)")
    return "\n".join(lines)
