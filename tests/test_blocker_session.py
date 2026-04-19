import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.blocker_session import BlockerSession


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=1,
        avg_error_rate=0.05,
        avg_throughput=200.0,
        status=PipelineStatus.HEALTHY,
    )


class TestBlockerSession:
    def setup_method(self):
        self.session = BlockerSession()

    def test_empty_session_returns_none(self):
        assert self.session.run() is None

    def test_pipeline_ids_empty_initially(self):
        assert self.session.pipeline_ids == []

    def test_add_snapshot_registers_pipeline(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in self.session.pipeline_ids

    def test_run_returns_result_with_snapshots(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        result = self.session.run()
        assert result is not None
        assert result.total_allowed == 1

    def test_blocked_pipeline_appears_in_blocked(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        self.session.block("pipe-a", "under review")
        result = self.session.run()
        assert result is not None
        assert result.total_blocked == 1
        assert result.blocked[0].pipeline_id == "pipe-a"

    def test_unblock_removes_from_blocked(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        self.session.block("pipe-a", "reason")
        self.session.unblock("pipe-a")
        result = self.session.run()
        assert result is not None
        assert result.total_blocked == 0
        assert result.total_allowed == 1
