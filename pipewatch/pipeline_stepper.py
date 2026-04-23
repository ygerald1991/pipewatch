from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class StepEntry:
    pipeline_id: str
    step_index: int
    step_name: str
    snapshot: PipelineSnapshot

    def __str__(self) -> str:
        return f"StepEntry(pipeline={self.pipeline_id}, step={self.step_index}:{self.step_name})"


@dataclass
class StepperResult:
    pipeline_id: str
    steps: List[StepEntry] = field(default_factory=list)

    @property
    def total_steps(self) -> int:
        return len(self.steps)

    @property
    def current_step(self) -> Optional[StepEntry]:
        return self.steps[-1] if self.steps else None

    def __str__(self) -> str:
        return f"StepperResult(pipeline={self.pipeline_id}, steps={self.total_steps})"


class PipelineStepper:
    def __init__(self, step_names: Optional[List[str]] = None) -> None:
        self._step_names: List[str] = step_names or []
        self._results: dict[str, StepperResult] = {}

    def _next_step_name(self, index: int) -> str:
        if index < len(self._step_names):
            return self._step_names[index]
        return f"step_{index}"

    def advance(self, snapshot: PipelineSnapshot) -> StepEntry:
        pid = snapshot.pipeline_id
        if pid not in self._results:
            self._results[pid] = StepperResult(pipeline_id=pid)
        result = self._results[pid]
        idx = result.total_steps
        entry = StepEntry(
            pipeline_id=pid,
            step_index=idx,
            step_name=self._next_step_name(idx),
            snapshot=snapshot,
        )
        result.steps.append(entry)
        return entry

    def result_for(self, pipeline_id: str) -> Optional[StepperResult]:
        return self._results.get(pipeline_id)

    def all_results(self) -> List[StepperResult]:
        return list(self._results.values())

    @property
    def pipeline_ids(self) -> List[str]:
        return list(self._results.keys())
