from datetime import datetime, timedelta
import pytest

from pipewatch.pipeline_silencer import PipelineSilencer, SilenceEntry
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
        metric_count=1,
    )


NOW = datetime(2024, 1, 1, 12, 0, 0)


class TestPipelineSilencer:
    def setup_method(self):
        self.silencer = PipelineSilencer()

    def test_new_silencer_has_no_active_entries(self):
        assert self.silencer.active_entries(NOW) == []

    def test_silence_marks_pipeline(self):
        self.silencer.silence("pipe-a", "maintenance", 300, now=NOW)
        assert self.silencer.is_silenced("pipe-a", NOW)

    def test_unsilenced_pipeline_not_silenced(self):
        assert not self.silencer.is_silenced("pipe-x", NOW)

    def test_silence_expires_after_duration(self):
        self.silencer.silence("pipe-a", "test", 60, now=NOW)
        future = NOW + timedelta(seconds=61)
        assert not self.silencer.is_silenced("pipe-a", future)

    def test_silence_active_within_duration(self):
        self.silencer.silence("pipe-a", "test", 300, now=NOW)
        future = NOW + timedelta(seconds=100)
        assert self.silencer.is_silenced("pipe-a", future)

    def test_lift_removes_silence(self):
        self.silencer.silence("pipe-a", "test", 300, now=NOW)
        result = self.silencer.lift("pipe-a")
        assert result is True
        assert not self.silencer.is_silenced("pipe-a", NOW)

    def test_lift_unknown_pipeline_returns_false(self):
        assert self.silencer.lift("no-such-pipe") is False

    def test_filter_snapshots_separates_silenced(self):
        self.silencer.silence("pipe-a", "test", 300, now=NOW)
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.silencer.filter_snapshots(snaps, now=NOW)
        assert "pipe-a" in result.silenced
        assert "pipe-b" in result.allowed

    def test_filter_snapshots_empty_list(self):
        result = self.silencer.filter_snapshots([], now=NOW)
        assert result.total_silenced == 0
        assert result.total_allowed == 0

    def test_active_entries_excludes_expired(self):
        self.silencer.silence("pipe-a", "test", 60, now=NOW)
        self.silencer.silence("pipe-b", "test", 300, now=NOW)
        future = NOW + timedelta(seconds=120)
        active = self.silencer.active_entries(future)
        ids = [e.pipeline_id for e in active]
        assert "pipe-b" in ids
        assert "pipe-a" not in ids

    def test_silence_entry_str_shows_status(self):
        entry = SilenceEntry("pipe-a", "test", NOW, 300)
        assert "active" in str(entry)
