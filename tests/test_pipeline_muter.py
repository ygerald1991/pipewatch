from __future__ import annotations
from datetime import datetime, timedelta
import pytest
from pipewatch.pipeline_muter import PipelineMuter, MuteEntry, MuterResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
        metric_count=5,
    )


class TestPipelineMuter:
    def setup_method(self):
        self.muter = PipelineMuter()

    def test_new_muter_has_no_active_entries(self):
        assert self.muter.active_entries() == []

    def test_mute_registers_pipeline(self):
        self.muter.mute("pipe-1", reason="maintenance")
        assert self.muter.is_muted("pipe-1")

    def test_unmuted_pipeline_not_muted(self):
        assert not self.muter.is_muted("pipe-x")

    def test_unmute_removes_entry(self):
        self.muter.mute("pipe-1")
        self.muter.unmute("pipe-1")
        assert not self.muter.is_muted("pipe-1")

    def test_expired_entry_not_active(self):
        past = datetime.utcnow() - timedelta(seconds=600)
        self.muter.mute("pipe-1", duration_seconds=10.0, now=past)
        assert not self.muter.is_muted("pipe-1")

    def test_active_entry_is_muted(self):
        self.muter.mute("pipe-1", duration_seconds=3600.0)
        assert self.muter.is_muted("pipe-1")

    def test_evaluate_returns_entries_for_known_pipelines(self):
        self.muter.mute("pipe-1")
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.muter.evaluate(snapshots)
        assert len(result.entries) == 1
        assert result.entries[0].pipeline_id == "pipe-1"

    def test_evaluate_empty_snapshots_returns_empty(self):
        self.muter.mute("pipe-1")
        result = self.muter.evaluate([])
        assert result.entries == []

    def test_total_muted_counts_active_only(self):
        past = datetime.utcnow() - timedelta(seconds=600)
        self.muter.mute("pipe-1", duration_seconds=3600.0)
        self.muter.mute("pipe-2", duration_seconds=10.0, now=past)
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.muter.evaluate(snapshots)
        assert result.total_muted == 1

    def test_is_muted_on_result(self):
        self.muter.mute("pipe-1")
        snapshots = [make_snapshot("pipe-1")]
        result = self.muter.evaluate(snapshots)
        assert result.is_muted("pipe-1")
        assert not result.is_muted("pipe-2")

    def test_mute_entry_str(self):
        entry = MuteEntry(
            pipeline_id="pipe-1",
            muted_at=datetime(2024, 1, 1, 12, 0, 0),
            duration_seconds=300.0,
            reason="test",
        )
        s = str(entry)
        assert "pipe-1" in s
        assert "test" in s
