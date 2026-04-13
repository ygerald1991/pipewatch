"""Snapshot reporter: summarises snapshot store state for CLI or export."""

from typing import List

from pipewatch.metrics import PipelineStatus
from pipewatch.snapshot import PipelineSnapshot, SnapshotStore


_STATUS_LABEL: dict = {
    PipelineStatus.HEALTHY: "HEALTHY",
    PipelineStatus.WARNING: "WARNING",
    PipelineStatus.CRITICAL: "CRITICAL",
    PipelineStatus.UNKNOWN: "UNKNOWN",
}


def _status_label(status: PipelineStatus) -> str:
    return _STATUS_LABEL.get(status, "UNKNOWN")


def format_snapshot_line(snap: PipelineSnapshot) -> str:
    """Return a single-line summary string for a snapshot."""
    return (
        f"[{_status_label(snap.status):8s}] "
        f"{snap.pipeline_id:<30s} "
        f"error_rate={snap.error_rate:.2%}  "
        f"throughput={snap.throughput:.1f}/s  "
        f"metrics={snap.metric_count}"
    )


def render_snapshot_table(store: SnapshotStore) -> str:
    """Render all snapshots in the store as a formatted text table."""
    snapshots = store.all()
    if not snapshots:
        return "No pipeline snapshots available."

    lines: List[str] = [
        "Pipeline Snapshot Summary",
        "=" * 70,
    ]
    for snap in sorted(snapshots, key=lambda s: s.pipeline_id):
        lines.append(format_snapshot_line(snap))
    lines.append("=" * 70)
    lines.append(f"Total pipelines: {len(snapshots)}")

    critical = sum(1 for s in snapshots if s.status == PipelineStatus.CRITICAL)
    warning = sum(1 for s in snapshots if s.status == PipelineStatus.WARNING)
    if critical:
        lines.append(f"  CRITICAL: {critical}")
    if warning:
        lines.append(f"  WARNING:  {warning}")

    return "\n".join(lines)


def overall_status(store: SnapshotStore) -> PipelineStatus:
    """Return the worst status across all tracked pipelines."""
    snapshots = store.all()
    if not snapshots:
        return PipelineStatus.UNKNOWN
    return max((s.status for s in snapshots), key=lambda st: st.value)
