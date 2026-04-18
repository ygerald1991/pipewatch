import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_limiter import limit_snapshots, LimiterResult
from pipewatch.limiter_session import LimiterSession


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=1,
        status="healthy",
    )


class TestLimitSnapshots:
    def test_empty_returns_empty(self):
        result = limit_snapshots([])
        assert result.results == []

    def test_single_pipeline_within_limit(self):
        snaps = [make_snapshot("p1") for _ in range(5)]
        result = limit_snapshots(snaps, default_limit=10)
        assert len(result.results) == 1
        r = result.results[0]
        assert r.pipeline_id == "p1"
        assert not r.limited
        assert r.dropped == 0
        assert r.snapshot_count == 5

    def test_single_pipeline_exceeds_limit(self):
        snaps = [make_snapshot("p1") for _ in range(15)]
        result = limit_snapshots(snaps, default_limit=10)
        r = result.results[0]
        assert r.limited
        assert r.dropped == 5
        assert r.snapshot_count == 10

    def test_override_applies_to_specific_pipeline(self):
        snaps = [make_snapshot("p1") for _ in range(20)]
        result = limit_snapshots(snaps, default_limit=100, overrides={"p1": 5})
        r = result.results[0]
        assert r.limited
        assert r.limit == 5
        assert r.dropped == 15

    def test_override_does_not_affect_other_pipelines(self):
        snaps = [make_snapshot("p1") for _ in range(3)] + [make_snapshot("p2") for _ in range(3)]
        result = limit_snapshots(snaps, default_limit=10, overrides={"p1": 2})
        by_id = {r.pipeline_id: r for r in result.results}
        assert by_id["p1"].limited
        assert not by_id["p2"].limited

    def test_total_dropped_sums_across_pipelines(self):
        snaps = [make_snapshot("p1") for _ in range(5)] + [make_snapshot("p2") for _ in range(5)]
        result = limit_snapshots(snaps, default_limit=3)
        assert result.total_dropped() == 4

    def test_limited_pipelines_filters_correctly(self):
        snaps = [make_snapshot("p1") for _ in range(2)] + [make_snapshot("p2") for _ in range(10)]
        result = limit_snapshots(snaps, default_limit=5)
        limited = result.limited_pipelines()
        assert len(limited) == 1
        assert limited[0].pipeline_id == "p2"


class TestLimiterSession:
    def test_empty_session_returns_no_results(self):
        session = LimiterSession()
        result = session.run()
        assert result.results == []

    def test_pipeline_ids_empty_initially(self):
        session = LimiterSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = LimiterSession()
        session.add_snapshot(make_snapshot("p1"))
        assert "p1" in session.pipeline_ids()

    def test_set_limit_applies_on_run(self):
        session = LimiterSession(default_limit=100)
        for _ in range(10):
            session.add_snapshot(make_snapshot("p1"))
        session.set_limit("p1", 3)
        result = session.run()
        r = result.results[0]
        assert r.limited
        assert r.limit == 3
