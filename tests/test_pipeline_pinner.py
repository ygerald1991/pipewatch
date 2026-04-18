from __future__ import annotations
import pytest
from datetime import datetime
from pipewatch.pipeline_pinner import PipelinePinner, PinEntry, PinnerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=5,
    )


class TestPipelinePinner:
    def setup_method(self):
        self.pinner = PipelinePinner()

    def test_new_pinner_has_no_pinned_pipelines(self):
        assert self.pinner.pinned_ids() == []

    def test_pin_marks_pipeline_as_pinned(self):
        self.pinner.pin("pipe-a", reason="critical")
        assert self.pinner.is_pinned("pipe-a")

    def test_pin_returns_entry(self):
        entry = self.pinner.pin("pipe-b", reason="test")
        assert isinstance(entry, PinEntry)
        assert entry.pipeline_id == "pipe-b"
        assert entry.reason == "test"

    def test_unpin_removes_pipeline(self):
        self.pinner.pin("pipe-a")
        result = self.pinner.unpin("pipe-a")
        assert result is True
        assert not self.pinner.is_pinned("pipe-a")

    def test_unpin_unknown_returns_false(self):
        assert self.pinner.unpin("nonexistent") is False

    def test_is_pinned_false_for_unknown(self):
        assert not self.pinner.is_pinned("unknown")

    def test_pinned_ids_returns_all_pinned(self):
        self.pinner.pin("a")
        self.pinner.pin("b")
        ids = self.pinner.pinned_ids()
        assert "a" in ids
        assert "b" in ids

    def test_evaluate_separates_pinned_and_unpinned(self):
        self.pinner.pin("pipe-a")
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.pinner.evaluate(snapshots)
        assert "pipe-a" in result.pinned
        assert "pipe-b" in result.unpinned

    def test_evaluate_empty_snapshots_returns_empty(self):
        result = self.pinner.evaluate([])
        assert result.total_pinned == 0
        assert result.total_unpinned == 0

    def test_total_pinned_count(self):
        self.pinner.pin("x")
        snapshots = [make_snapshot("x"), make_snapshot("y")]
        result = self.pinner.evaluate(snapshots)
        assert result.total_pinned == 1
        assert result.total_unpinned == 1

    def test_pin_accepts_custom_timestamp(self):
        ts = datetime(2024, 1, 1, 12, 0, 0)
        entry = self.pinner.pin("pipe-z", pinned_at=ts)
        assert entry.pinned_at == ts

    def test_pin_same_pipeline_twice_does_not_duplicate(self):
        """Re-pinning an already pinned pipeline should not create duplicate entries."""
        self.pinner.pin("pipe-a", reason="first")
        self.pinner.pin("pipe-a", reason="second")
        ids = self.pinner.pinned_ids()
        assert ids.count("pipe-a") == 1
