from __future__ import annotations
from typing import List
from pipewatch.pipeline_router import RouterResult


def _format_row(pipeline_id: str, destination: str, matched: bool) -> str:
    rule_label = "rule" if matched else "default"
    return f"  {pipeline_id:<30} {destination:<20} {rule_label}"


def render_router_table(result: RouterResult) -> str:
    if not result.routes:
        return "No routes resolved."

    lines: List[str] = [
        f"  {'Pipeline':<30} {'Destination':<20} {'Match'}",
        "  " + "-" * 56,
    ]
    for route in result.routes:
        lines.append(_format_row(
            route.pipeline_id,
            route.destination,
            route.matched_rule is not None,
        ))
    return "\n".join(lines)


def destination_summary(result: RouterResult) -> str:
    if not result.routes:
        return "No destinations."
    dest_counts: dict[str, int] = {}
    for route in result.routes:
        dest_counts[route.destination] = dest_counts.get(route.destination, 0) + 1
    parts = [f"{dest}={count}" for dest, count in sorted(dest_counts.items())]
    return "Destinations: " + ", ".join(parts)
