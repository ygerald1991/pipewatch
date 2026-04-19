import pytest
from datetime import datetime
from pipewatch.blacklister_session import BlacklisterSession
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=1,
        avg_error_rate=0.05,
        avg_throughput=50.0,
        status=PipelineStatus.HEALTHY,
    )


class TestBlacklisterSession:
    def setup_method(self):
        self.session = BlacklisterSession()

    def test_empty_session_returns_none(self):
        assert self.session.run() is None

    def test_pipeline_ids_empty_initially(self):
        assert self.session.pipeline_ids == []

    def test_add_snapshot_registers_pipeline(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in self.session.pipeline_ids

    def test_blacklisted_pipeline_blocked(self):
        self.session.blacklist("pipe-a", reason="test")
        self.session.add_snapshot(make_snapshot("pipe-a"))
        self.session.add_snapshot(make_snapshot("pipe-b"))
        result = self.session.run()
        assert result is not None
        assert result.total_blocked == 1
        assert result.total_allowed == 1

    def test_unblacklist_allows_pipeline(self):
        self.session.blacklist("pipe-a")
        self.session.unblacklist("pipe-a")
        self.session.add_snapshot(make_snapshot("pipe-a"))
        result = self.session.run()
        assert result is not None
        assert result.total_blocked == 0
        assert result.total_allowed == 1
