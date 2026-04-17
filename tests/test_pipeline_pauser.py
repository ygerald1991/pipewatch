import pytest
from datetime import datetime, timezone
from pipewatch.pipeline_pauser import PipelinePauser, PauseEntry, PauserResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(timezone.utc),
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=5,
    )


class TestPipelinePauser:
    def setup_method(self):
        self.pauser = PipelinePauser()

    def test_new_pauser_has_no_paused_pipelines(self):
        assert not self.pauser.is_paused("pipe-1")

    def test_pause_marks_pipeline_as_paused(self):
        self.pauser.pause("pipe-1", reason="maintenance")
        assert self.pauser.is_paused("pipe-1")

    def test_resume_clears_paused_state(self):
        self.pauser.pause("pipe-1")
        self.pauser.resume("pipe-1")
        assert not self.pauser.is_paused("pipe-1")

    def test_resume_unknown_pipeline_returns_none(self):
        result = self.pauser.resume("unknown")
        assert result is None

    def test_pause_entry_has_correct_pipeline_id(self):
        entry = self.pauser.pause("pipe-2", reason="test")
        assert entry.pipeline_id == "pipe-2"

    def test_pause_entry_is_active(self):
        entry = self.pauser.pause("pipe-3")
        assert entry.is_active

    def test_resumed_entry_is_not_active(self):
        self.pauser.pause("pipe-4")
        entry = self.pauser.resume("pipe-4")
        assert entry is not None
        assert not entry.is_active

    def test_entry_for_returns_none_when_not_paused(self):
        assert self.pauser.entry_for("pipe-x") is None

    def test_entry_for_returns_entry_after_pause(self):
        self.pauser.pause("pipe-5", reason="overload")
        entry = self.pauser.entry_for("pipe-5")
        assert entry is not None
        assert entry.reason == "overload"

    def test_evaluate_separates_paused_and_active(self):
        self.pauser.pause("pipe-a")
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.pauser.evaluate(snaps)
        assert "pipe-a" in result.paused
        assert "pipe-b" in result.active

    def test_evaluate_empty_snapshots_returns_empty_result(self):
        result = self.pauser.evaluate([])
        assert result.total_paused == 0
        assert result.active == []

    def test_total_paused_counts_active_pauses_only(self):
        self.pauser.pause("pipe-1")
        self.pauser.pause("pipe-2")
        self.pauser.resume("pipe-2")
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.pauser.evaluate(snaps)
        assert result.total_paused == 1

    def test_str_shows_paused_status(self):
        entry = self.pauser.pause("pipe-z")
        assert "paused" in str(entry)
        assert "pipe-z" in str(entry)
