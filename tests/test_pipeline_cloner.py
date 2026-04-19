import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.pipeline_cloner import clone_snapshots, ClonerResult, CloneResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    metric = PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        success_count=100,
        failure_count=int(error_rate * 100),
        records_processed=200,
        duration_seconds=10.0,
    )
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metrics=[metric],
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestCloneSnapshots:
    def test_empty_returns_empty(self):
        result = clone_snapshots({}, {})
        assert isinstance(result, ClonerResult)
        assert result.clones == []

    def test_single_pipeline_cloned(self):
        snap = make_snapshot("pipe-a")
        result = clone_snapshots({"pipe-a": [snap]}, {"pipe-a": "pipe-a-clone"})
        assert len(result.clones) == 1
        clone = result.clones[0]
        assert clone.source_id == "pipe-a"
        assert clone.clone_id == "pipe-a-clone"

    def test_cloned_snapshot_has_new_pipeline_id(self):
        snap = make_snapshot("pipe-a")
        result = clone_snapshots({"pipe-a": [snap]}, {"pipe-a": "pipe-a-clone"})
        cloned_snap = result.clones[0].snapshots[0]
        assert cloned_snap.pipeline_id == "pipe-a-clone"

    def test_original_snapshot_unchanged(self):
        snap = make_snapshot("pipe-a")
        clone_snapshots({"pipe-a": [snap]}, {"pipe-a": "pipe-a-clone"})
        assert snap.pipeline_id == "pipe-a"

    def test_snapshot_count_matches(self):
        snaps = [make_snapshot("pipe-b") for _ in range(3)]
        result = clone_snapshots({"pipe-b": snaps}, {"pipe-b": "pipe-b-copy"})
        assert result.clones[0].snapshot_count == 3

    def test_total_cloned_sums_all(self):
        snaps_a = [make_snapshot("a") for _ in range(2)]
        snaps_b = [make_snapshot("b") for _ in range(4)]
        result = clone_snapshots(
            {"a": snaps_a, "b": snaps_b},
            {"a": "a-clone", "b": "b-clone"},
        )
        assert result.total_cloned() == 6

    def test_pipeline_ids_returns_clone_ids(self):
        snaps = [make_snapshot("x")]
        result = clone_snapshots({"x": snaps}, {"x": "x-new"})
        assert "x-new" in result.pipeline_ids()

    def test_for_clone_returns_correct_result(self):
        snaps = [make_snapshot("src")]
        result = clone_snapshots({"src": snaps}, {"src": "dst"})
        found = result.for_clone("dst")
        assert found is not None
        assert found.clone_id == "dst"

    def test_for_clone_returns_none_when_missing(self):
        result = clone_snapshots({}, {})
        assert result.for_clone("nonexistent") is None

    def test_source_not_in_id_map_is_ignored(self):
        snaps = [make_snapshot("orphan")]
        result = clone_snapshots({"orphan": snaps}, {})
        assert result.clones == []

    def test_str_representation(self):
        snaps = [make_snapshot("p")]
        result = clone_snapshots({"p": snaps}, {"p": "p2"})
        s = str(result.clones[0])
        assert "p" in s
        assert "p2" in s
