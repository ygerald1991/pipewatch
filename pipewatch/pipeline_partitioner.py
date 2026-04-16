from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class PartitionResult:
    key: str
    snapshots: List[PipelineSnapshot] = field(default_factory=list)

    def __str__(self) -> str:
        return f"Partition({self.key!r}, count={len(self.snapshots)})"

    @property
    def size(self) -> int:
        return len(self.snapshots)

    @property
    def avg_error_rate(self) -> Optional[float]:
        rates = [s.error_rate for s in self.snapshots if s.error_rate is not None]
        return sum(rates) / len(rates) if rates else None

    @property
    def avg_throughput(self) -> Optional[float]:
        values = [s.throughput for s in self.snapshots if s.throughput is not None]
        return sum(values) / len(values) if values else None


def partition_by_status(snapshots: List[PipelineSnapshot]) -> Dict[str, PartitionResult]:
    partitions: Dict[str, PartitionResult] = {}
    for snap in snapshots:
        key = snap.status.value if snap.status is not None else "unknown"
        if key not in partitions:
            partitions[key] = PartitionResult(key=key)
        partitions[key].snapshots.append(snap)
    return partitions


def partition_by_tag(snapshots: List[PipelineSnapshot], tag_registry=None) -> Dict[str, PartitionResult]:
    partitions: Dict[str, PartitionResult] = {}
    for snap in snapshots:
        tags = tag_registry.tags_for(snap.pipeline_id) if tag_registry else []
        if not tags:
            bucket = "untagged"
            if bucket not in partitions:
                partitions[bucket] = PartitionResult(key=bucket)
            partitions[bucket].snapshots.append(snap)
        for tag in tags:
            if tag not in partitions:
                partitions[tag] = PartitionResult(key=tag)
            partitions[tag].snapshots.append(snap)
    return partitions


def partition_by_error_band(snapshots: List[PipelineSnapshot]) -> Dict[str, PartitionResult]:
    bands = {"healthy": PartitionResult(key="healthy"),
             "warning": PartitionResult(key="warning"),
             "critical": PartitionResult(key="critical"),
             "unknown": PartitionResult(key="unknown")}
    for snap in snapshots:
        rate = snap.error_rate
        if rate is None:
            bands["unknown"].snapshots.append(snap)
        elif rate < 0.05:
            bands["healthy"].snapshots.append(snap)
        elif rate < 0.15:
            bands["warning"].snapshots.append(snap)
        else:
            bands["critical"].snapshots.append(snap)
    return {k: v for k, v in bands.items() if v.size > 0}
