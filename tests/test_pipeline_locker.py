import pytest
from datetime import datetime, timezone
from pipewatch.pipeline_locker import PipelineLocker, LockEntry, LockerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
        metric_count=5,
        captured_at=datetime.now(timezone.utc),
    )


class TestPipelineLocker:
    def setup_method(self):
        self.locker = PipelineLocker()

    def test_new_locker_has_no_locked_pipelines(self):
        assert self.locker.locked_pipeline_ids == []

    def test_lock_marks_pipeline(self):
        self.locker.lock("pipe-1", locked_by="admin")
        assert self.locker.is_locked("pipe-1")

    def test_lock_returns_entry(self):
        entry = self.locker.lock("pipe-1", locked_by="admin", reason="maintenance")
        assert isinstance(entry, LockEntry)
        assert entry.pipeline_id == "pipe-1"
        assert entry.locked_by == "admin"
        assert entry.reason == "maintenance"

    def test_unlock_removes_lock(self):
        self.locker.lock("pipe-1", locked_by="admin")
        result = self.locker.unlock("pipe-1")
        assert result is True
        assert not self.locker.is_locked("pipe-1")

    def test_unlock_returns_false_when_not_locked(self):
        assert self.locker.unlock("pipe-99") is False

    def test_is_locked_returns_false_for_unknown(self):
        assert not self.locker.is_locked("pipe-unknown")

    def test_lock_entry_returns_none_for_unlocked(self):
        assert self.locker.lock_entry("pipe-x") is None

    def test_lock_entry_returns_correct_entry(self):
        self.locker.lock("pipe-2", locked_by="ci", reason="deploy")
        entry = self.locker.lock_entry("pipe-2")
        assert entry is not None
        assert entry.locked_by == "ci"

    def test_evaluate_separates_locked_and_unlocked(self):
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2"), make_snapshot("pipe-3")]
        self.locker.lock("pipe-1", locked_by="admin")
        self.locker.lock("pipe-3", locked_by="bot")
        result = self.locker.evaluate(snaps)
        assert result.total_locked == 2
        assert result.total_unlocked == 1

    def test_evaluate_empty_snapshots_returns_empty(self):
        result = self.locker.evaluate([])
        assert result.total_locked == 0
        assert result.total_unlocked == 0

    def test_pipeline_ids_lists_locked(self):
        self.locker.lock("pipe-A", locked_by="user")
        snaps = [make_snapshot("pipe-A"), make_snapshot("pipe-B")]
        result = self.locker.evaluate(snaps)
        assert "pipe-A" in result.pipeline_ids
        assert "pipe-B" not in result.pipeline_ids
