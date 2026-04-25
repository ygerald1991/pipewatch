from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class GateEntry:
    pipeline_id: str
    allowed: bool
    reason: str

    def __str__(self) -> str:
        status = "ALLOWED" if self.allowed else "BLOCKED"
        return f"[{status}] {self.pipeline_id}: {self.reason}"


@dataclass
class GatekeeperResult:
    entries: List[GateEntry] = field(default_factory=list)

    @property
    def total_allowed(self) -> int:
        return sum(1 for e in self.entries if e.allowed)

    @property
    def total_blocked(self) -> int:
        return sum(1 for e in self.entries if not e.allowed)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def allowed_pipelines(self) -> List[str]:
        return [e.pipeline_id for e in self.entries if e.allowed]

    def blocked_pipelines(self) -> List[str]:
        return [e.pipeline_id for e in self.entries if not e.allowed]


class PipelineGatekeeper:
    def __init__(
        self,
        max_error_rate: float = 0.5,
        min_throughput: float = 0.0,
        overrides: Optional[Dict[str, bool]] = None,
    ) -> None:
        self._max_error_rate = max_error_rate
        self._min_throughput = min_throughput
        self._overrides: Dict[str, bool] = overrides or {}

    def evaluate(self, snapshot: PipelineSnapshot) -> GateEntry:
        pid = snapshot.pipeline_id
        if pid in self._overrides:
            allowed = self._overrides[pid]
            reason = "manual override"
            return GateEntry(pipeline_id=pid, allowed=allowed, reason=reason)

        error_rate = snapshot.error_rate
        throughput = snapshot.throughput

        if error_rate is not None and error_rate > self._max_error_rate:
            return GateEntry(
                pipeline_id=pid,
                allowed=False,
                reason=f"error_rate {error_rate:.2%} exceeds max {self._max_error_rate:.2%}",
            )
        if throughput is not None and throughput < self._min_throughput:
            return GateEntry(
                pipeline_id=pid,
                allowed=False,
                reason=f"throughput {throughput:.2f} below min {self._min_throughput:.2f}",
            )
        return GateEntry(pipeline_id=pid, allowed=True, reason="within thresholds")

    def run(self, snapshots: List[PipelineSnapshot]) -> GatekeeperResult:
        result = GatekeeperResult()
        for snapshot in snapshots:
            result.entries.append(self.evaluate(snapshot))
        return result
