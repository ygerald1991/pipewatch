from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.pipeline_scheduler import PipelineScheduler, SchedulerResult


class SchedulerSession:
    """Coordinates scheduling across multiple pipelines."""

    def __init__(self, default_interval_seconds: int = 60) -> None:
        self._scheduler = PipelineScheduler()
        self._default_interval = default_interval_seconds
        self._history: List[SchedulerResult] = []

    def register(
        self,
        pipeline_id: str,
        interval_seconds: Optional[int] = None,
    ) -> None:
        interval = interval_seconds if interval_seconds is not None else self._default_interval
        self._scheduler.register(pipeline_id, interval)

    def register_many(
        self,
        pipeline_ids: List[str],
        interval_seconds: Optional[int] = None,
    ) -> None:
        for pid in pipeline_ids:
            self.register(pid, interval_seconds)

    def disable(self, pipeline_id: str) -> None:
        self._scheduler.disable(pipeline_id)

    def enable(self, pipeline_id: str) -> None:
        self._scheduler.enable(pipeline_id)

    def tick(self, now: Optional[datetime] = None) -> SchedulerResult:
        result = self._scheduler.tick(now)
        self._history.append(result)
        return result

    def due_pipelines(self, now: Optional[datetime] = None) -> List[str]:
        return self._scheduler.due_pipelines(now)

    def pipeline_ids(self) -> List[str]:
        return self._scheduler.pipeline_ids()

    def history(self) -> List[SchedulerResult]:
        return list(self._history)

    def total_triggered(self) -> int:
        return sum(len(r.triggered) for r in self._history)

    @property
    def scheduler(self) -> PipelineScheduler:
        return self._scheduler
