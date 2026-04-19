import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_blocker import PipelineBlocker, BlockEntry, BlockerResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=1,
        avg_error_rate=error_rate,
        avg_throughput=100.0,
        status=PipelineStatus.HEALTHY,
    )


class TestPipelineBlocker:
    def setup_method(self):
        self.blocker = PipelineBlocker()

    def test_new_blocker_has_no_blocked_pipelines(self):
        assert self.blocker.blocked_pipeline_ids == []

    def test_block_marks_pipeline(self):
        self.blocker.block("pipe-a", "maintenance")
        assert self.blocker.is_blocked("pipe-a")

    def test_block_returns_entry(self):
        entry = self.blocker.block("pipe-a", "maintenance")
        assert isinstance(entry, BlockEntry)
        assert entry.pipeline_id == "pipe-a"
        assert entry.reason == "maintenance"

    def test_unblock_removes_pipeline(self):
        self.blocker.block("pipe-a", "reason")
        result = self.blocker.unblock("pipe-a")
        assert result is True
        assert not self.blocker.is_blocked("pipe-a")

    def test_unblock_unknown_pipeline_returns_false(self):
        assert self.blocker.unblock("nonexistent") is False

    def test_entry_for_returns_entry(self):
        self.blocker.block("pipe-a", "test reason")
        entry = self.blocker.entry_for("pipe-a")
        assert entry is not None
        assert entry.reason == "test reason"

    def test_entry_for_unknown_returns_none(self):
        assert self.blocker.entry_for("ghost") is None

    def test_apply_separates_blocked_and_allowed(self):
        self.blocker.block("pipe-b", "down")
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.blocker.apply(snapshots)
        assert result.total_blocked == 1
        assert result.total_allowed == 1
        assert result.blocked[0].pipeline_id == "pipe-b"
        assert result.allowed[0].pipeline_id == "pipe-a"

    def test_apply_empty_snapshots_returns_empty_result(self):
        result = self.blocker.apply([])
        assert result.total_blocked == 0
        assert result.total_allowed == 0

    def test_apply_no_blocks_allows_all(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.blocker.apply(snapshots)
        assert result.total_blocked == 0
        assert result.total_allowed == 2
