import pytest
from datetime import datetime
from pipewatch.pipeline_freezer import PipelineFreezer, FreezeEntry, FreezerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=5,
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
    )


class TestPipelineFreezer:
    def setup_method(self):
        self.freezer = PipelineFreezer()

    def test_new_freezer_has_no_frozen_pipelines(self):
        assert self.freezer.all_frozen == []

    def test_freeze_returns_entry(self):
        snap = make_snapshot("pipe-1")
        entry = self.freezer.freeze(snap)
        assert isinstance(entry, FreezeEntry)
        assert entry.pipeline_id == "pipe-1"

    def test_is_frozen_returns_true_after_freeze(self):
        snap = make_snapshot("pipe-1")
        self.freezer.freeze(snap)
        assert self.freezer.is_frozen("pipe-1") is True

    def test_is_frozen_returns_false_for_unknown(self):
        assert self.freezer.is_frozen("unknown") is False

    def test_unfreeze_removes_entry(self):
        snap = make_snapshot("pipe-1")
        self.freezer.freeze(snap)
        result = self.freezer.unfreeze("pipe-1")
        assert result is True
        assert self.freezer.is_frozen("pipe-1") is False

    def test_unfreeze_returns_false_for_unknown(self):
        assert self.freezer.unfreeze("ghost") is False

    def test_frozen_entry_returns_entry(self):
        snap = make_snapshot("pipe-2")
        self.freezer.freeze(snap, reason="test-reason")
        entry = self.freezer.frozen_entry("pipe-2")
        assert entry is not None
        assert entry.reason == "test-reason"

    def test_frozen_entry_returns_none_for_unknown(self):
        assert self.freezer.frozen_entry("nope") is None

    def test_freeze_snapshots_bulk(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(3)]
        result = self.freezer.freeze_snapshots(snaps, reason="bulk")
        assert isinstance(result, FreezerResult)
        assert result.total_frozen == 3
        assert result.skipped == []

    def test_freeze_snapshots_skips_already_frozen(self):
        snap = make_snapshot("pipe-1")
        self.freezer.freeze(snap)
        result = self.freezer.freeze_snapshots([snap])
        assert result.total_frozen == 0
        assert "pipe-1" in result.skipped

    def test_pipeline_ids_in_result(self):
        snaps = [make_snapshot("a"), make_snapshot("b")]
        result = self.freezer.freeze_snapshots(snaps)
        assert set(result.pipeline_ids) == {"a", "b"}

    def test_str_representation(self):
        snap = make_snapshot("pipe-x")
        entry = self.freezer.freeze(snap, reason="debug")
        assert "pipe-x" in str(entry)
        assert "debug" in str(entry)
