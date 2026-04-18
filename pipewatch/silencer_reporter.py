from __future__ import annotations
from typing import List

from pipewatch.pipeline_silencer import SilenceEntry, SilencerResult


def _format_row(entry: SilenceEntry) -> str:
    status = "ACTIVE" if entry.is_active() else "EXPIRED"
    return (
        f"  {entry.pipeline_id:<30} {status:<10} "
        f"expires={entry.expires_at.strftime('%Y-%m-%dT%H:%M:%S')}  reason={entry.reason}"
    )


def render_silencer_table(entries: List[SilenceEntry]) -> str:
    if not entries:
        return "No silenced pipelines."
    header = f"  {'PIPELINE':<30} {'STATUS':<10} DETAILS"
    separator = "  " + "-" * 70
    rows = [_format_row(e) for e in entries]
    return "\n".join([header, separator] + rows)


def silencer_summary(result: SilencerResult) -> str:
    lines = [
        f"Silenced : {result.total_silenced}",
        f"Allowed  : {result.total_allowed}",
    ]
    return "\n".join(lines)
