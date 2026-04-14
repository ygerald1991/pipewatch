"""Tests for pipewatch.correlation_session."""

from __future__ import annotations

from typing import Dict

from pipewatch.correlation_session import CorrelationSession
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, metrics: Dict[str, float]) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metrics=metrics,
        metric_count=len(metrics),
        error_rate=metrics.get("error_rate", 0.0),
        throughput=metrics.get("throughput", 0.0),
        status="healthy",
    )


class TestCorrelationSession:
    def _build_session(self):
        session = CorrelationSession(metrics=["error_rate", "throughput"])
        for i in range(4):
            v = float(i + 1)
            session.add_snapshot(make_snapshot("pipe-a", {"error_rate": v * 0.1, "throughput": v * 10}))
            session.add_snapshot(make_snapshot("pipe-b", {"error_rate": v * 0.1, "throughput": v * 10}))
        return session

    def test_pipeline_ids_registered(self):
        session = self._build_session()
        assert set(session.pipeline_ids()) == {"pipe-a", "pipe-b"}

    def test_run_returns_results(self):
        session = self._build_session()
        results = session.run()
        assert len(results) > 0

    def test_significant_results_subset_of_run(self):
        session = self._build_session()
        all_results = session.run()
        sig = session.significant_results()
        assert all(r.is_significant for r in sig)
        assert len(sig) <= len(all_results)

    def test_empty_session_run_returns_empty(self):
        session = CorrelationSession()
        assert session.run() == []

    def test_single_pipeline_no_pairs(self):
        session = CorrelationSession()
        for i in range(4):
            session.add_snapshot(make_snapshot("only-pipe", {"error_rate": float(i) * 0.1}))
        assert session.run() == []

    def test_three_pipelines_produces_three_pairs(self):
        session = CorrelationSession(metrics=["error_rate"])
        for pid in ["a", "b", "c"]:
            for i in range(4):
                session.add_snapshot(make_snapshot(pid, {"error_rate": float(i) * 0.1}))
        # 3 choose 2 = 3 pairs, each with 1 metric
        results = session.run()
        assert len(results) == 3
