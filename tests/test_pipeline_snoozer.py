from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_snoozer import PipelineSnoozer, SnoozeEntry, SnoozerResult
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        avg_error_rate=error_rate,
        avg_throughput=100.0,
        status="healthy",
        captured_at=datetime.utcnow(),
    )


class TestPipelineSnoozer:
    def setup_method(self):
        self.snoozer = PipelineSnoozer()

    def test_new_snoozer_has_no_active_entries(self):
        result = self.snoozer.all_entries()
        assert result.total_snoozed == 0

    def test_snooze_registers_pipeline(self):
        self.snoozer.snooze("pipe-1", duration_seconds=300)
        assert self.snoozer.is_snoozed("pipe-1")

    def test_unregistered_pipeline_not_snoozed(self):
        assert not self.snoozer.is_snoozed("pipe-unknown")

    def test_snooze_returns_entry(self):
        entry = self.snoozer.snooze("pipe-1", duration_seconds=60, reason="maintenance")
        assert isinstance(entry, SnoozeEntry)
        assert entry.pipeline_id == "pipe-1"
        assert entry.reason == "maintenance"

    def test_expired_snooze_not_active(self):
        entry = self.snoozer.snooze("pipe-1", duration_seconds=10)
        future = datetime.utcnow() + timedelta(seconds=20)
        assert not entry.is_active(now=future)

    def test_active_snooze_is_active(self):
        entry = self.snoozer.snooze("pipe-1", duration_seconds=300)
        assert entry.is_active()

    def test_unsnooze_removes_pipeline(self):
        self.snoozer.snooze("pipe-1", duration_seconds=300)
        removed = self.snoozer.unsnooze("pipe-1")
        assert removed is True
        assert not self.snoozer.is_snoozed("pipe-1")

    def test_unsnooze_missing_pipeline_returns_false(self):
        result = self.snoozer.unsnooze("pipe-missing")
        assert result is False

    def test_run_returns_entries_for_snoozed_snapshots(self):
        self.snoozer.snooze("pipe-1", duration_seconds=300)
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.snoozer.run(snapshots)
        assert len(result.entries) == 1
        assert result.entries[0].pipeline_id == "pipe-1"

    def test_run_returns_empty_when_no_snoozes(self):
        snapshots = [make_snapshot("pipe-1")]
        result = self.snoozer.run(snapshots)
        assert result.total_snoozed == 0

    def test_total_expired_counts_correctly(self):
        entry = self.snoozer.snooze("pipe-1", duration_seconds=1)
        past = datetime.utcnow() - timedelta(seconds=5)
        entry.snoozed_at = past
        result = self.snoozer.all_entries()
        assert result.total_expired == 1
        assert result.total_snoozed == 0

    def test_pipeline_ids_in_result(self):
        self.snoozer.snooze("pipe-a", duration_seconds=60)
        self.snoozer.snooze("pipe-b", duration_seconds=60)
        result = self.snoozer.all_entries()
        assert set(result.pipeline_ids) == {"pipe-a", "pipe-b"}
