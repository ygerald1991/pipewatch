"""Persist and retrieve pipeline baseline snapshots used for drift detection."""

from typing import Dict, Optional

from pipewatch.snapshot import PipelineSnapshot


class BaselineStore:
    """In-memory store for baseline snapshots, keyed by pipeline_id."""

    def __init__(self) -> None:
        self._baselines: Dict[str, PipelineSnapshot] = {}

    def set(self, snapshot: PipelineSnapshot) -> None:
        """Store *snapshot* as the baseline for its pipeline."""
        self._baselines[snapshot.pipeline_id] = snapshot

    def get(self, pipeline_id: str) -> Optional[PipelineSnapshot]:
        """Return the baseline snapshot for *pipeline_id*, or None."""
        return self._baselines.get(pipeline_id)

    def remove(self, pipeline_id: str) -> None:
        """Remove the baseline for *pipeline_id* if it exists."""
        self._baselines.pop(pipeline_id, None)

    def pipeline_ids(self) -> list:
        return list(self._baselines.keys())

    def __len__(self) -> int:
        return len(self._baselines)

    def __contains__(self, pipeline_id: str) -> bool:
        return pipeline_id in self._baselines
