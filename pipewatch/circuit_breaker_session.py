from typing import Dict, List, Optional
from pipewatch.pipeline_circuit_breaker import PipelineCircuitBreaker, BreakerResult
from pipewatch.snapshot import PipelineSnapshot


class CircuitBreakerSession:
    def __init__(self, threshold: int = 3, cooldown_seconds: int = 60, error_threshold: float = 0.1):
        self._breaker = PipelineCircuitBreaker(threshold=threshold, cooldown_seconds=cooldown_seconds)
        self._error_threshold = error_threshold
        self._snapshots: Dict[str, List[PipelineSnapshot]] = {}

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        pid = snapshot.pipeline_id
        if pid not in self._snapshots:
            self._snapshots[pid] = []
        self._snapshots[pid].append(snapshot)

    def pipeline_ids(self) -> List[str]:
        return list(self._snapshots.keys())

    def run(self) -> Dict[str, BreakerResult]:
        results: Dict[str, BreakerResult] = {}
        for pid, snapshots in self._snapshots.items():
            latest = snapshots[-1] if snapshots else None
            if latest is not None:
                results[pid] = self._breaker.evaluate(latest, self._error_threshold)
        return results

    def tripped_pipelines(self) -> List[str]:
        return [pid for pid, r in self.run().items() if r.tripped]
