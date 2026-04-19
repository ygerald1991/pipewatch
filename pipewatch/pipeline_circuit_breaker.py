from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CircuitState:
    pipeline_id: str
    open: bool = False
    failure_count: int = 0
    opened_at: Optional[datetime] = None
    last_checked: Optional[datetime] = None

    def __str__(self) -> str:
        status = "OPEN" if self.open else "CLOSED"
        return f"CircuitState({self.pipeline_id}, {status}, failures={self.failure_count})"


@dataclass
class BreakerResult:
    pipeline_id: str
    tripped: bool
    failure_count: int
    state: str

    def __str__(self) -> str:
        return f"{self.pipeline_id}: {'TRIPPED' if self.tripped else 'OK'} (failures={self.failure_count})"


class PipelineCircuitBreaker:
    def __init__(self, threshold: int = 3, cooldown_seconds: int = 60):
        self._threshold = threshold
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._states: Dict[str, CircuitState] = {}

    def _get_state(self, pipeline_id: str) -> CircuitState:
        if pipeline_id not in self._states:
            self._states[pipeline_id] = CircuitState(pipeline_id=pipeline_id)
        return self._states[pipeline_id]

    def record_failure(self, pipeline_id: str) -> None:
        state = self._get_state(pipeline_id)
        state.failure_count += 1
        state.last_checked = datetime.utcnow()
        if state.failure_count >= self._threshold and not state.open:
            state.open = True
            state.opened_at = datetime.utcnow()

    def record_success(self, pipeline_id: str) -> None:
        state = self._get_state(pipeline_id)
        state.failure_count = 0
        state.open = False
        state.opened_at = None

    def is_open(self, pipeline_id: str) -> bool:
        state = self._get_state(pipeline_id)
        if state.open and state.opened_at:
            if datetime.utcnow() - state.opened_at > self._cooldown:
                state.open = False
                state.failure_count = 0
        return state.open

    def evaluate(self, snapshot: PipelineSnapshot, error_threshold: float = 0.1) -> BreakerResult:
        pid = snapshot.pipeline_id
        error_rate = snapshot.error_rate if snapshot.error_rate is not None else 0.0
        if error_rate >= error_threshold:
            self.record_failure(pid)
        else:
            self.record_success(pid)
        open_state = self.is_open(pid)
        state = self._get_state(pid)
        return BreakerResult(
            pipeline_id=pid,
            tripped=open_state,
            failure_count=state.failure_count,
            state="OPEN" if open_state else "CLOSED",
        )

    def pipeline_ids(self) -> List[str]:
        return list(self._states.keys())
