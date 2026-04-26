from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class CensorEntry:
    pipeline_id: str
    redacted_fields: List[str]

    def __str__(self) -> str:
        fields = ", ".join(self.redacted_fields) if self.redacted_fields else "none"
        return f"CensorEntry(pipeline={self.pipeline_id}, redacted=[{fields}])"


@dataclass
class CensorResult:
    pipeline_id: str
    original_error_rate: Optional[float]
    original_throughput: Optional[float]
    censored_fields: List[str]
    error_rate: Optional[float]
    throughput: Optional[float]

    def __str__(self) -> str:
        return (
            f"CensorResult(pipeline={self.pipeline_id}, "
            f"censored={self.censored_fields})"
        )


@dataclass
class CensorerResult:
    entries: List[CensorResult] = field(default_factory=list)

    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.entries]

    def total_censored(self) -> int:
        return sum(1 for e in self.entries if e.censored_fields)

    def for_pipeline(self, pipeline_id: str) -> Optional[CensorResult]:
        for e in self.entries:
            if e.pipeline_id == pipeline_id:
                return e
        return None


class PipelineCensorer:
    def __init__(self) -> None:
        self._rules: Dict[str, Set[str]] = {}

    def censor(self, pipeline_id: str, fields: List[str]) -> CensorEntry:
        self._rules.setdefault(pipeline_id, set()).update(fields)
        return CensorEntry(pipeline_id=pipeline_id, redacted_fields=list(fields))

    def uncensor(self, pipeline_id: str, fields: Optional[List[str]] = None) -> None:
        if pipeline_id not in self._rules:
            return
        if fields is None:
            del self._rules[pipeline_id]
        else:
            self._rules[pipeline_id] -= set(fields)
            if not self._rules[pipeline_id]:
                del self._rules[pipeline_id]

    def censored_fields_for(self, pipeline_id: str) -> List[str]:
        return sorted(self._rules.get(pipeline_id, set()))

    def apply(self, snapshots: List[PipelineSnapshot]) -> CensorerResult:
        results: List[CensorResult] = []
        for snap in snapshots:
            redacted = self._rules.get(snap.pipeline_id, set())
            error_rate = None if "error_rate" in redacted else snap.error_rate
            throughput = None if "throughput" in redacted else snap.throughput
            results.append(
                CensorResult(
                    pipeline_id=snap.pipeline_id,
                    original_error_rate=snap.error_rate,
                    original_throughput=snap.throughput,
                    censored_fields=sorted(redacted),
                    error_rate=error_rate,
                    throughput=throughput,
                )
            )
        return CensorerResult(entries=results)
