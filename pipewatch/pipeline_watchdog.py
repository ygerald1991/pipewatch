"""Watchdog that detects pipelines that have gone silent (no recent metrics)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.pipeline_registry import PipelineRegistry


@dataclass
class SilenceResult:
    pipeline_id: str
    last_seen: Optional[datetime]
    silence_threshold_seconds: float
    is_silent: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            return f"{self.pipeline_id}: never seen (silent)"
        age = (datetime.utcnow() - self.last_seen).total_seconds()
        status = "SILENT" if self.is_silent else "OK"
        return f"{self.pipeline_id}: last seen {age:.1f}s ago [{status}]"


@dataclass
class WatchdogReport:
    results: List[SilenceResult] = field(default_factory=list)

    @property
    def silent_pipelines(self) -> List[SilenceResult]:
        return [r for r in self.results if r.is_silent]

    @property
    def healthy_pipelines(self) -> List[SilenceResult]:
        return [r for r in self.results if not r.is_silent]

    @property
    def all_healthy(self) -> bool:
        return len(self.silent_pipelines) == 0


def _last_seen(registry: PipelineRegistry, pipeline_id: str) -> Optional[datetime]:
    collector = registry.collector_for(pipeline_id)
    if collector is None:
        return None
    latest = collector.latest()
    if latest is None:
        return None
    return latest.timestamp


def run_watchdog(
    registry: PipelineRegistry,
    silence_threshold_seconds: float = 300.0,
    pipeline_overrides: Optional[Dict[str, float]] = None,
) -> WatchdogReport:
    """Check all registered pipelines for silence.

    Args:
        registry: The pipeline registry to inspect.
        silence_threshold_seconds: Default max age (seconds) before a pipeline
            is considered silent.
        pipeline_overrides: Optional per-pipeline threshold overrides.

    Returns:
        A WatchdogReport containing one SilenceResult per pipeline.
    """
    overrides = pipeline_overrides or {}
    now = datetime.utcnow()
    report = WatchdogReport()

    for pid in registry.pipeline_ids():
        threshold = overrides.get(pid, silence_threshold_seconds)
        last = _last_seen(registry, pid)

        if last is None:
            is_silent = True
        else:
            age = (now - last).total_seconds()
            is_silent = age > threshold

        report.results.append(
            SilenceResult(
                pipeline_id=pid,
                last_seen=last,
                silence_threshold_seconds=threshold,
                is_silent=is_silent,
            )
        )

    return report
