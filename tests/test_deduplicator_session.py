import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.deduplicator_session import DeduplicatorSession


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=5,
        status="healthy",
    )


class TestDeduplicatorSession:
    def test_empty_session_returns_no_results(self):
        session = DeduplicatorSession()
        assert session.run() == []

    def test_pipeline_ids_empty_initially(self):
        session = DeduplicatorSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = DeduplicatorSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        assert "pipe-1" in session.pipeline_ids()

    def test_run_returns_result_per_pipeline(self):
        session = DeduplicatorSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        session.add_snapshot(make_snapshot("pipe-2"))
        results = session.run()
        assert len(results) == 2

    def test_total_removed_counts_all_duplicates(self):
        session = DeduplicatorSession()
        snap = make_snapshot("pipe-1")
        session.add_snapshot(snap)
        session.add_snapshot(snap)
        session.add_snapshot(snap)
        assert session.total_removed() == 2

    def test_total_removed_zero_when_no_duplicates(self):
        session = DeduplicatorSession()
        session.add_snapshot(make_snapshot("pipe-1", error_rate=0.1))
        session.add_snapshot(make_snapshot("pipe-1", error_rate=0.2))
        assert session.total_removed() == 0

    def test_multiple_pipelines_deduplicated_independently(self):
        session = DeduplicatorSession()
        snap_a = make_snapshot("pipe-a")
        snap_b = make_snapshot("pipe-b", error_rate=0.5)
        session.add_snapshot(snap_a)
        session.add_snapshot(snap_a)
        session.add_snapshot(snap_b)
        results = {r.pipeline_id: r for r in session.run()}
        assert results["pipe-a"].duplicates_removed == 1
        assert results["pipe-b"].duplicates_removed == 0
