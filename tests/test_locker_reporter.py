import pytest
from datetime import datetime, timezone
from pipewatch.pipeline_locker import PipelineLocker
from pipewatch.locker_reporter import render_locker_table, locked_summary
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.01,
        throughput=50.0,
        metric_count=3,
        captured_at=datetime.now(timezone.utc),
    )


class TestRenderLockerTable:
    def setup_method(self):
        self.locker = PipelineLocker()

    def test_empty_snapshots_returns_no_pipelines_message(self):
        result = self.locker.evaluate([])
        output = render_locker_table(result)
        assert "No pipelines" in output

    def test_none_result_returns_no_pipelines_message(self):
        output = render_locker_table(None)
        assert "No pipelines" in output

    def test_output_contains_pipeline_id(self):
        snaps = [make_snapshot("pipe-alpha")]
        result = self.locker.evaluate(snaps)
        output = render_locker_table(result)
        assert "pipe-alpha" in output

    def test_locked_pipeline_shows_locked_status(self):
        snaps = [make_snapshot("pipe-1")]
        self.locker.lock("pipe-1", locked_by="admin", reason="deploy")
        result = self.locker.evaluate(snaps)
        output = render_locker_table(result)
        assert "LOCKED" in output

    def test_unlocked_pipeline_shows_unlocked_status(self):
        snaps = [make_snapshot("pipe-2")]
        result = self.locker.evaluate(snaps)
        output = render_locker_table(result)
        assert "unlocked" in output

    def test_locked_by_shown_in_output(self):
        snaps = [make_snapshot("pipe-3")]
        self.locker.lock("pipe-3", locked_by="ci-bot")
        result = self.locker.evaluate(snaps)
        output = render_locker_table(result)
        assert "ci-bot" in output

    def test_locked_summary_counts_correctly(self):
        snaps = [make_snapshot("p1"), make_snapshot("p2"), make_snapshot("p3")]
        self.locker.lock("p1", locked_by="x")
        result = self.locker.evaluate(snaps)
        summary = locked_summary(result)
        assert "1 locked" in summary
        assert "2 unlocked" in summary

    def test_locked_summary_none_returns_message(self):
        output = locked_summary(None)
        assert "No locker result" in output
