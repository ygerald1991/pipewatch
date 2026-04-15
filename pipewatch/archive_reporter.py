"""Render archive history as human-readable tables."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_archiver import ArchiveEntry, ArchiveStore

_COL_WIDTHS = (24, 30, 12, 12)
_HEADER = (
    f"{'Pipeline':<24} {'Archived At':<30} {'Error Rate':>12} {'Throughput':>12}"
)
_DIVIDER = "-" * sum(_COL_WIDTHS)


def _format_row(entry: ArchiveEntry) -> str:
    snap = entry.snapshot
    error_rate = snap.get("error_rate")
    throughput = snap.get("throughput")
    er_str = f"{error_rate:.4f}" if error_rate is not None else "N/A"
    tp_str = f"{throughput:.2f}" if throughput is not None else "N/A"
    return (
        f"{entry.pipeline_id:<24} "
        f"{entry.archived_at:<30} "
        f"{er_str:>12} "
        f"{tp_str:>12}"
    )


def render_archive_table(store: ArchiveStore, pipeline_id: str) -> str:
    entries = store.entries_for(pipeline_id)
    if not entries:
        return f"No archive entries for pipeline '{pipeline_id}'."
    lines: List[str] = [_HEADER, _DIVIDER]
    for entry in reversed(entries):
        lines.append(_format_row(entry))
    return "\n".join(lines)


def archive_summary(store: ArchiveStore) -> str:
    pids = store.pipeline_ids()
    if not pids:
        return "Archive is empty."
    lines = [f"{'Pipeline':<24} {'Entries':>8}"]
    lines.append("-" * 34)
    for pid in sorted(pids):
        count = len(store.entries_for(pid))
        lines.append(f"{pid:<24} {count:>8}")
    lines.append("-" * 34)
    lines.append(f"{'Total':<24} {store.total_entries():>8}")
    return "\n".join(lines)
