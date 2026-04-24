from __future__ import annotations

import time
from datetime import datetime

import pytest

from pipewatch.pipeline_tracker import PipelineTracker, TrackerResult
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str = "pipe-1", error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=5,
        captured_at=datetime.utcnow(),
    )


class TestPipelineTracker:
    def setup_method(self):
        self.tracker = PipelineTracker()

    def test_new_tracker_has_no_pipeline_ids(self):
        assert self.tracker.pipeline_ids == []

    def test_track_registers_pipeline(self):
        snap = make_snapshot("pipe-a")
        self.tracker.track(snap)
        assert "pipe-a" in self.tracker.pipeline_ids

    def test_track_returns_entry_with_correct_pipeline_id(self):
        snap = make_snapshot("pipe-b")
        entry = self.tracker.track(snap)
        assert entry.pipeline_id == "pipe-b"

    def test_track_stores_label(self):
        snap = make_snapshot("pipe-c")
        entry = self.tracker.track(snap, label="production")
        assert entry.label == "production"

    def test_latest_returns_none_for_unknown_pipeline(self):
        assert self.tracker.latest("unknown") is None

    def test_latest_returns_most_recent_entry(self):
        snap = make_snapshot("pipe-d")
        self.tracker.track(snap, label="first")
        time.sleep(0.01)
        self.tracker.track(snap, label="second")
        latest = self.tracker.latest("pipe-d")
        assert latest is not None
        assert latest.label == "second"

    def test_history_returns_all_entries(self):
        snap = make_snapshot("pipe-e")
        self.tracker.track(snap, label="v1")
        self.tracker.track(snap, label="v2")
        history = self.tracker.history("pipe-e")
        assert len(history) == 2

    def test_history_returns_empty_for_unknown_pipeline(self):
        assert self.tracker.history("nonexistent") == []

    def test_untrack_removes_pipeline(self):
        snap = make_snapshot("pipe-f")
        self.tracker.track(snap)
        removed = self.tracker.untrack("pipe-f")
        assert removed == 1
        assert "pipe-f" not in self.tracker.pipeline_ids

    def test_untrack_unknown_pipeline_returns_zero(self):
        assert self.tracker.untrack("ghost") == 0

    def test_result_total_tracked_counts_all_entries(self):
        snap_a = make_snapshot("pipe-g")
        snap_b = make_snapshot("pipe-h")
        self.tracker.track(snap_a)
        self.tracker.track(snap_a)
        self.tracker.track(snap_b)
        result = self.tracker.result()
        assert result.total_tracked == 3

    def test_result_pipeline_ids_are_unique(self):
        snap = make_snapshot("pipe-i")
        self.tracker.track(snap)
        self.tracker.track(snap)
        result = self.tracker.result()
        assert result.pipeline_ids.count("pipe-i") == 1

    def test_result_entries_for_filters_correctly(self):
        snap_a = make_snapshot("pipe-j")
        snap_b = make_snapshot("pipe-k")
        self.tracker.track(snap_a, label="alpha")
        self.tracker.track(snap_b, label="beta")
        result = self.tracker.result()
        entries = result.entries_for("pipe-j")
        assert len(entries) == 1
        assert entries[0].label == "alpha"

    def test_str_representation_includes_pipeline_id(self):
        snap = make_snapshot("pipe-l")
        entry = self.tracker.track(snap, label="debug")
        assert "pipe-l" in str(entry)
        assert "debug" in str(entry)
