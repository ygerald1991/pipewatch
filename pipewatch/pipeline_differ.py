"""Computes field-level diffs between two snapshots of the same pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class FieldDiff:
    field_name: str
    before: Optional[float]
    after: Optional[float]
    delta: Optional[float]
    pct_change: Optional[float]

    def __str__(self) -> str:
        delta_str = f"{self.delta:+.4f}" if self.delta is not None else "N/A"
        pct_str = f"{self.pct_change:+.1f}%" if self.pct_change is not None else "N/A"
        return f"{self.field_name}: {self.before} -> {self.after} ({delta_str}, {pct_str})"


@dataclass
class DiffResult:
    pipeline_id: str
    diffs: List[FieldDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(
            d.delta is not None and abs(d.delta) > 1e-9
            for d in self.diffs
        )

    @property
    def changed_fields(self) -> List[str]:
        return [
            d.field_name
            for d in self.diffs
            if d.delta is not None and abs(d.delta) > 1e-9
        ]


def _pct_change(before: Optional[float], after: Optional[float]) -> Optional[float]:
    if before is None or after is None:
        return None
    if before == 0.0:
        return None
    return ((after - before) / abs(before)) * 100.0


def diff_snapshots(before: PipelineSnapshot, after: PipelineSnapshot) -> DiffResult:
    """Return a DiffResult describing per-field changes between two snapshots."""
    if before.pipeline_id != after.pipeline_id:
        raise ValueError(
            f"Cannot diff snapshots from different pipelines: "
            f"{before.pipeline_id!r} vs {after.pipeline_id!r}"
        )

    fields: Dict[str, tuple] = {
        "error_rate": (before.error_rate, after.error_rate),
        "throughput": (before.throughput, after.throughput),
        "avg_latency": (before.avg_latency, after.avg_latency),
    }

    diffs: List[FieldDiff] = []
    for fname, (bval, aval) in fields.items():
        if bval is None and aval is None:
            delta = None
        elif bval is None or aval is None:
            delta = None
        else:
            delta = aval - bval
        diffs.append(
            FieldDiff(
                field_name=fname,
                before=bval,
                after=aval,
                delta=delta,
                pct_change=_pct_change(bval, aval),
            )
        )

    return DiffResult(pipeline_id=before.pipeline_id, diffs=diffs)
