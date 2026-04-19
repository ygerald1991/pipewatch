from dataclasses import dataclass, field
from typing import Optional, List, Dict
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class FlagEntry:
    pipeline_id: str
    reason: str
    severity: str = "warning"

    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.pipeline_id}: {self.reason}"


@dataclass
class FlaggerResult:
    entries: List[FlagEntry] = field(default_factory=list)

    @property
    def total_flagged(self) -> int:
        return len(self.entries)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def for_pipeline(self, pipeline_id: str) -> Optional[FlagEntry]:
        for entry in self.entries:
            if entry.pipeline_id == pipeline_id:
                return entry
        return None

    def by_severity(self, severity: str) -> List[FlagEntry]:
        return [e for e in self.entries if e.severity == severity]


class PipelineFlagger:
    def __init__(
        self,
        error_rate_warning: float = 0.05,
        error_rate_critical: float = 0.15,
        min_throughput: float = 1.0,
    ):
        self._error_rate_warning = error_rate_warning
        self._error_rate_critical = error_rate_critical
        self._min_throughput = min_throughput

    def flag(self, snapshots: List[PipelineSnapshot]) -> FlaggerResult:
        entries: List[FlagEntry] = []
        for snap in snapshots:
            entry = self._evaluate(snap)
            if entry is not None:
                entries.append(entry)
        return FlaggerResult(entries=entries)

    def _evaluate(self, snap: PipelineSnapshot) -> Optional[FlagEntry]:
        er = snap.error_rate
        tp = snap.throughput

        if er is not None and er >= self._error_rate_critical:
            return FlagEntry(snap.pipeline_id, f"error rate {er:.2%} exceeds critical threshold", "critical")
        if er is not None and er >= self._error_rate_warning:
            return FlagEntry(snap.pipeline_id, f"error rate {er:.2%} exceeds warning threshold", "warning")
        if tp is not None and tp < self._min_throughput:
            return FlagEntry(snap.pipeline_id, f"throughput {tp:.2f} below minimum {self._min_throughput:.2f}", "warning")
        return None
