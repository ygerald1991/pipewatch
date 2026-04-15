"""Tests for pipewatch.pipeline_watchdog."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.pipeline_watchdog import (
    SilenceResult,
    WatchdogReport,
    run_watchdog,
)


def make_metric(pipeline_id: str, timestamp: datetime) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=timestamp,
        records_processed=100,
        records_failed=0,
        duration_seconds=10.0,
        status=PipelineStatus.HEALTHY,
    )


def _registry_with(*pipeline_ids: str, last_seen_offset: float = 0.0) -> PipelineRegistry:
    """Build a registry where each pipeline has a metric `last_seen_offset` seconds ago."""
    registry = PipelineRegistry()
    now = datetime.utcnow()
    for pid in pipeline_ids:
        ts = now - timedelta(seconds=last_seen_offset)
        registry.record(make_metric(pid, ts))
    return registry


class TestWatchdogReport:
    def test_all_healthy_when_no_silent(self):
        report = WatchdogReport(
            results=[
                SilenceResult("p1", datetime.utcnow(), 300.0, False),
                SilenceResult("p2", datetime.utcnow(), 300.0, False),
            ]
        )
        assert report.all_healthy is True

    def test_not_all_healthy_when_one_silent(self):
        report = WatchdogReport(
            results=[
                SilenceResult("p1", datetime.utcnow(), 300.0, False),
                SilenceResult("p2", None, 300.0, True),
            ]
        )
        assert report.all_healthy is False
        assert len(report.silent_pipelines) == 1
        assert report.silent_pipelines[0].pipeline_id == "p2"

    def test_healthy_pipelines_excludes_silent(self):
        report = WatchdogReport(
            results=[
                SilenceResult("p1", datetime.utcnow(), 300.0, False),
                SilenceResult("p2", None, 300.0, True),
            ]
        )
        assert len(report.healthy_pipelines) == 1
        assert report.healthy_pipelines[0].pipeline_id == "p1"


class TestRunWatchdog:
    def test_empty_registry_returns_empty_report(self):
        registry = PipelineRegistry()
        report = run_watchdog(registry)
        assert report.results == []
        assert report.all_healthy is True

    def test_recent_metric_not_silent(self):
        registry = _registry_with("pipe-a", last_seen_offset=10.0)
        report = run_watchdog(registry, silence_threshold_seconds=300.0)
        assert len(report.results) == 1
        assert report.results[0].is_silent is False

    def test_old_metric_is_silent(self):
        registry = _registry_with("pipe-b", last_seen_offset=400.0)
        report = run_watchdog(registry, silence_threshold_seconds=300.0)
        assert report.results[0].is_silent is True

    def test_pipeline_override_threshold(self):
        registry = _registry_with("pipe-c", last_seen_offset=60.0)
        # default 300s would not flag it, but override of 30s should
        report = run_watchdog(
            registry,
            silence_threshold_seconds=300.0,
            pipeline_overrides={"pipe-c": 30.0},
        )
        assert report.results[0].is_silent is True
        assert report.results[0].silence_threshold_seconds == 30.0

    def test_multiple_pipelines_mixed_status(self):
        registry = PipelineRegistry()
        now = datetime.utcnow()
        registry.record(make_metric("healthy", now - timedelta(seconds=10)))
        registry.record(make_metric("silent", now - timedelta(seconds=600)))
        report = run_watchdog(registry, silence_threshold_seconds=300.0)
        assert len(report.results) == 2
        assert report.all_healthy is False
        assert len(report.silent_pipelines) == 1
        assert report.silent_pipelines[0].pipeline_id == "silent"

    def test_silence_result_str_never_seen(self):
        result = SilenceResult("p1", None, 300.0, True)
        assert "never seen" in str(result)
        assert "p1" in str(result)
