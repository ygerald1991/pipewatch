import pytest
from datetime import datetime, timedelta
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_pruner import prune_snapshots, PruneResult, PrunerResult
from pipewatch.pruner_session import PrunerSession


def make_snapshot(pipeline_id: str, offset_seconds: int = 0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds),
        error_rate=0.01,
        throughput=100.0,
        metric_count=5,
    )


class TestPruneSnapshots:
    def test_empty_returns_empty(self):
        result = prune_snapshots([])
        assert result.results == []
        assert result.total_pruned == 0
        assert result.total_kept == 0

    def test_single_snapshot_kept(self):
        snaps = [make_snapshot("p1")]
        result = prune_snapshots(snaps, max_per_pipeline=10)
        assert len(result.results) == 1
        assert result.results[0].kept_count == 1
        assert result.results[0].pruned_count == 0

    def test_excess_snapshots_pruned(self):
        snaps = [make_snapshot("p1", i) for i in range(15)]
        result = prune_snapshots(snaps, max_per_pipeline=10)
        assert result.results[0].kept_count == 10
        assert result.results[0].pruned_count == 5

    def test_most_recent_kept(self):
        snaps = [make_snapshot("p1", i * 10) for i in range(5)]
        result = prune_snapshots(snaps, max_per_pipeline=3)
        assert result.results[0].kept_count == 3
        assert result.results[0].pruned_count == 2

    def test_multiple_pipelines_tracked_separately(self):
        snaps = (
            [make_snapshot("p1", i) for i in range(12)] +
            [make_snapshot("p2", i) for i in range(5)]
        )
        result = prune_snapshots(snaps, max_per_pipeline=10)
        ids = result.pipeline_ids()
        assert "p1" in ids
        assert "p2" in ids
        p1 = next(r for r in result.results if r.pipeline_id == "p1")
        p2 = next(r for r in result.results if r.pipeline_id == "p2")
        assert p1.pruned_count == 2
        assert p2.pruned_count == 0

    def test_total_pruned_sums_across_pipelines(self):
        snaps = (
            [make_snapshot("a", i) for i in range(12)] +
            [make_snapshot("b", i) for i in range(11)]
        )
        result = prune_snapshots(snaps, max_per_pipeline=10)
        assert result.total_pruned == 3

    def test_str_representation(self):
        r = PruneResult(pipeline_id="x", original_count=5, pruned_count=2, kept_count=3)
        assert "x" in str(r)
        assert "pruned=2" in str(r)


class TestPrunerSession:
    def test_empty_session_returns_empty(self):
        session = PrunerSession()
        result = session.run()
        assert result.results == []

    def test_pipeline_ids_registered(self):
        session = PrunerSession()
        session.add_snapshot(make_snapshot("pipe-a"))
        session.add_snapshot(make_snapshot("pipe-b"))
        assert "pipe-a" in session.pipeline_ids()
        assert "pipe-b" in session.pipeline_ids()

    def test_run_prunes_correctly(self):
        session = PrunerSession(max_per_pipeline=3)
        for i in range(6):
            session.add_snapshot(make_snapshot("p1", i))
        result = session.run()
        assert result.results[0].kept_count == 3
        assert result.results[0].pruned_count == 3

    def test_last_result_stored(self):
        session = PrunerSession()
        assert session.last_result is None
        session.run()
        assert session.last_result is not None
