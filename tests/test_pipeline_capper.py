import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_capper import cap_snapshots, CapperResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=1,
    )


class TestCapSnapshots:
    def test_empty_returns_empty(self):
        result = cap_snapshots([])
        assert result.results == []
        assert result.total_dropped == 0

    def test_single_snapshot_kept(self):
        snaps = [make_snapshot("p1")]
        result = cap_snapshots(snaps, max_per_pipeline=5)
        cap = result.for_pipeline("p1")
        assert cap is not None
        assert cap.capped_count == 1
        assert cap.original_count == 1
        assert not cap.was_capped

    def test_excess_snapshots_capped(self):
        snaps = [make_snapshot("p1") for _ in range(15)]
        result = cap_snapshots(snaps, max_per_pipeline=10)
        cap = result.for_pipeline("p1")
        assert cap.original_count == 15
        assert cap.capped_count == 10
        assert cap.was_capped
        assert len(cap.snapshots) == 10

    def test_total_dropped_is_correct(self):
        snaps = [make_snapshot("p1") for _ in range(12)] + \
                [make_snapshot("p2") for _ in range(8)]
        result = cap_snapshots(snaps, max_per_pipeline=10)
        assert result.total_dropped == 2

    def test_multiple_pipelines_capped_independently(self):
        snaps = [make_snapshot("p1") for _ in range(5)] + \
                [make_snapshot("p2") for _ in range(20)]
        result = cap_snapshots(snaps, max_per_pipeline=10)
        p1 = result.for_pipeline("p1")
        p2 = result.for_pipeline("p2")
        assert p1.capped_count == 5
        assert p2.capped_count == 10

    def test_pipeline_ids_returned(self):
        snaps = [make_snapshot("alpha"), make_snapshot("beta")]
        result = cap_snapshots(snaps)
        ids = result.pipeline_ids()
        assert "alpha" in ids
        assert "beta" in ids

    def test_for_pipeline_returns_none_when_missing(self):
        result = cap_snapshots([])
        assert result.for_pipeline("nonexistent") is None

    def test_str_representation(self):
        snaps = [make_snapshot("p1")]
        result = cap_snapshots(snaps)
        cap = result.for_pipeline("p1")
        assert "p1" in str(cap)
