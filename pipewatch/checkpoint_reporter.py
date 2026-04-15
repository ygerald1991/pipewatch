"""Render checkpoint status as a human-readable table."""
from typing import List
from pipewatch.checkpoint import Checkpoint, CheckpointStore

_COL_ID = 20
_COL_STAGE = 18
_COL_STATUS = 8
_COL_MSG = 30


def _format_row(cp: Checkpoint) -> str:
    status = "OK" if cp.success else "FAIL"
    ts = cp.timestamp.strftime("%H:%M:%S")
    msg = (cp.message[:27] + "...") if len(cp.message) > 30 else cp.message
    return (
        f"{cp.pipeline_id:<{_COL_ID}}"
        f"{cp.stage:<{_COL_STAGE}}"
        f"{status:<{_COL_STATUS}}"
        f"{ts:<10}"
        f"{msg}"
    ).rstrip()


def render_checkpoint_table(store: CheckpointStore) -> str:
    ids = store.pipeline_ids()
    if not ids:
        return "No checkpoint data available."

    header = (
        f"{'PIPELINE':<{_COL_ID}}"
        f"{'STAGE':<{_COL_STAGE}}"
        f"{'STATUS':<{_COL_STATUS}}"
        f"{'TIME':<10}"
        f"MESSAGE"
    )
    separator = "-" * (len(header) + 10)
    lines = [header, separator]

    for pid in sorted(ids):
        for cp in store.history(pid):
            lines.append(_format_row(cp))

    return "\n".join(lines)


def overall_checkpoint_status(store: CheckpointStore) -> str:
    ids = store.pipeline_ids()
    if not ids:
        return "UNKNOWN"
    if all(store.all_passed(pid) for pid in ids):
        return "HEALTHY"
    return "DEGRADED"
