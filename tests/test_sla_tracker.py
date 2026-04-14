"""Tests for pipewatch.sla_tracker and pipewatch.sla_session."""

from __future__ import annotations

import time
import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.sla_tracker import SLAPolicy, SLAResult, evaluate_sla
from pipewatch.sla_session import SLASession


def make_metric(
    pipeline_id: str = "pipe-1",
    processed: int = 100,
    failed: int = 0,
    duration: float = 10.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=time.time(),
        records_processed=processed,
        records_failed=failed,
        duration_seconds=duration,
        status=PipelineStatus.OK,
    )


# ---------------------------------------------------------------------------
# SLAPolicy
# ---------------------------------------------------------------------------

class TestSLAPolicy:
    def test_defaults_are_sensible(self):
        p = SLAPolicy()
        assert 0 < p.max_error_rate < 1
        assert p.min_throughput > 0

    def test_for_pipeline_returns_defaults_when_no_override(self):
        p = SLAPolicy(max_error_rate=0.05, min_throughput=2.0)
        effective = p.for_pipeline("unknown-pipe")
        assert effective.max_error_rate == 0.05
        assert effective.min_throughput == 2.0

    def test_for_pipeline_applies_override(self):
        p = SLAPolicy(
            max_error_rate=0.05,
            min_throughput=2.0,
            overrides={"pipe-x": {"max_error_rate": 0.10}},
        )
        effective = p.for_pipeline("pipe-x")
        assert effective.max_error_rate == 0.10
        assert effective.min_throughput == 2.0  # unchanged


# ---------------------------------------------------------------------------
# evaluate_sla
# ---------------------------------------------------------------------------

class TestEvaluateSLA:
    def test_returns_none_for_empty_metrics(self):
        assert evaluate_sla("pipe-1", []) is None

    def test_healthy_pipeline_meets_sla(self):
        metrics = [make_metric(processed=100, failed=0, duration=10.0)]
        result = evaluate_sla("pipe-1", metrics)
        assert result is not None
        assert result.met is True
        assert result.error_rate_ok is True
        assert result.throughput_ok is True

    def test_high_error_rate_breaches_sla(self):
        metrics = [make_metric(processed=100, failed=50, duration=10.0)]
        policy = SLAPolicy(max_error_rate=0.05, min_throughput=1.0)
        result = evaluate_sla("pipe-1", metrics, policy)
        assert result is not None
        assert result.met is False
        assert result.error_rate_ok is False

    def test_low_throughput_breaches_sla(self):
        # 1 record in 100 seconds => 0.01 /s, well below default 1.0
        metrics = [make_metric(processed=1, failed=0, duration=100.0)]
        policy = SLAPolicy(max_error_rate=0.05, min_throughput=1.0)
        result = evaluate_sla("pipe-1", metrics, policy)
        assert result is not None
        assert result.met is False
        assert result.throughput_ok is False

    def test_str_contains_pipeline_id(self):
        metrics = [make_metric(processed=100, failed=0, duration=10.0)]
        result = evaluate_sla("pipe-1", metrics)
        assert "pipe-1" in str(result)


# ---------------------------------------------------------------------------
# SLASession
# ---------------------------------------------------------------------------

class TestSLASession:
    def _make_registry(self, metrics_by_pipeline: dict) -> PipelineRegistry:
        registry = PipelineRegistry()
        for pid, m_list in metrics_by_pipeline.items():
            for m in m_list:
                registry.record(pid, m)
        return registry

    def test_empty_registry_returns_no_results(self):
        session = SLASession(PipelineRegistry())
        assert session.run() == []

    def test_run_returns_one_result_per_pipeline(self):
        registry = self._make_registry({
            "p1": [make_metric("p1")],
            "p2": [make_metric("p2")],
        })
        session = SLASession(registry)
        results = session.run()
        assert len(results) == 2

    def test_breaching_pipelines_filters_correctly(self):
        registry = self._make_registry({
            "good": [make_metric("good", processed=100, failed=0, duration=10.0)],
            "bad": [make_metric("bad", processed=100, failed=80, duration=10.0)],
        })
        policy = SLAPolicy(max_error_rate=0.05, min_throughput=1.0)
        session = SLASession(registry, policy)
        session.run()
        breaches = session.breaching_pipelines()
        assert len(breaches) == 1
        assert breaches[0].pipeline_id == "bad"
