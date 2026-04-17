from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class NormalizeResult:
    pipeline_id: str
    original_error_rate: Optional[float]
    original_throughput: Optional[float]
    normalized_error_rate: Optional[float]
    normalized_throughput: Optional[float]

    def __str__(self) -> str:
        return (
            f"NormalizeResult(pipeline={self.pipeline_id}, "
            f"error_rate={self.normalized_error_rate:.4f}, "
            f"throughput={self.normalized_throughput:.4f})"
        )


@dataclass
class NormalizerResult:
    results: list[NormalizeResult] = field(default_factory=list)

    def pipeline_ids(self) -> list[str]:
        return [r.pipeline_id for r in self.results]

    def for_pipeline(self, pipeline_id: str) -> Optional[NormalizeResult]:
        for r in self.results:
            if r.pipeline_id == pipeline_id:
                return r
        return None


def _safe_normalize(value: Optional[float], min_val: float, max_val: float) -> Optional[float]:
    if value is None:
        return None
    span = max_val - min_val
    if span == 0.0:
        return 0.0
    return (value - min_val) / span


def normalize_snapshots(snapshots: list[PipelineSnapshot]) -> NormalizerResult:
    if not snapshots:
        return NormalizerResult()

    error_rates = [s.error_rate for s in snapshots if s.error_rate is not None]
    throughputs = [s.throughput for s in snapshots if s.throughput is not None]

    min_er, max_er = (min(error_rates), max(error_rates)) if error_rates else (0.0, 0.0)
    min_tp, max_tp = (min(throughputs), max(throughputs)) if throughputs else (0.0, 0.0)

    results = []
    for s in snapshots:
        results.append(NormalizeResult(
            pipeline_id=s.pipeline_id,
            original_error_rate=s.error_rate,
            original_throughput=s.throughput,
            normalized_error_rate=_safe_normalize(s.error_rate, min_er, max_er),
            normalized_throughput=_safe_normalize(s.throughput, min_tp, max_tp),
        ))

    return NormalizerResult(results=results)
