import pytest
from datetime import datetime
from pipewatch.pipeline_blacklister import PipelineBlacklister, BlacklistEntry
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=1,
        avg_error_rate=error_rate,
        avg_throughput=100.0,
        status=PipelineStatus.HEALTHY,
    )


class TestPipelineBlacklister:
    def setup_method(self):
        self.bl = PipelineBlacklister()

    def test_new_blacklister_has_no_entries(self):
        assert self.bl.blacklisted_ids == []

    def test_blacklist_marks_pipeline(self):
        self.bl.blacklist("pipe-a", reason="spam")
        assert self.bl.is_blacklisted("pipe-a")

    def test_unblacklist_removes_entry(self):
        self.bl.blacklist("pipe-a")
        result = self.bl.unblacklist("pipe-a")
        assert result is True
        assert not self.bl.is_blacklisted("pipe-a")

    def test_unblacklist_unknown_returns_false(self):
        assert self.bl.unblacklist("ghost") is False

    def test_apply_blocks_blacklisted_pipeline(self):
        self.bl.blacklist("pipe-a", reason="test")
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.bl.apply(snaps)
        assert result.total_blocked == 1
        assert result.total_allowed == 1

    def test_apply_empty_snapshots_returns_empty(self):
        result = self.bl.apply([])
        assert result.total_blocked == 0
        assert result.total_allowed == 0

    def test_allowed_pipeline_not_in_blocked(self):
        self.bl.blacklist("pipe-a")
        snaps = [make_snapshot("pipe-b")]
        result = self.bl.apply(snaps)
        assert "pipe-b" not in result.pipeline_ids
        assert result.total_allowed == 1

    def test_entry_reason_stored(self):
        self.bl.blacklist("pipe-x", reason="compliance")
        snaps = [make_snapshot("pipe-x")]
        result = self.bl.apply(snaps)
        assert result.entries[0].reason == "compliance"
