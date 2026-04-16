import pytest
from datetime import datetime, timedelta
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.tracer_session import TracerSession


def make_snapshot(pipeline_id: str, offset: int = 0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset),
        metric_count=2,
        avg_error_rate=0.02,
        avg_throughput=50.0,
        status="healthy",
    )


class TestTracerSession:
    def test_empty_session_returns_no_results(self):
        session = TracerSession()
        assert session.run() == []

    def test_pipeline_ids_empty_initially(self):
        session = TracerSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = TracerSession()
        session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in session.pipeline_ids()

    def test_run_returns_one_result_per_pipeline(self):
        session = TracerSession()
        for i in range(3):
            session.add_snapshot(make_snapshot("pipe-a", i * 10))
        for i in range(2):
            session.add_snapshot(make_snapshot("pipe-b", i * 5))
        results = session.run()
        ids = {r.pipeline_id for r in results}
        assert ids == {"pipe-a", "pipe-b"}

    def test_result_for_unknown_pipeline_returns_none(self):
        session = TracerSession()
        assert session.result_for("ghost") is None

    def test_result_for_known_pipeline_returns_trace(self):
        session = TracerSession()
        session.add_snapshot(make_snapshot("pipe-x", 0))
        session.add_snapshot(make_snapshot("pipe-x", 20))
        result = session.result_for("pipe-x")
        assert result is not None
        assert result.pipeline_id == "pipe-x"
        assert result.stage_count == 2

    def test_snapshots_sorted_by_captured_at(self):
        session = TracerSession()
        session.add_snapshot(make_snapshot("pipe-y", 30))
        session.add_snapshot(make_snapshot("pipe-y", 0))
        session.add_snapshot(make_snapshot("pipe-y", 15))
        result = session.result_for("pipe-y")
        durations = [s.duration_seconds for s in result.spans]
        assert all(d >= 0 for d in durations if d is not None)
