from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

from pipewatch.pipeline_inhibitor import InhibitEntry, InhibitorResult, PipelineInhibitor
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=1,
    )


class TestPipelineInhibitor:
    def setup_method(self):
        self.inhibitor = PipelineInhibitor()

    def test_new_inhibitor_has_no_entries(self):
        assert self.inhibitor.inhibited_pipeline_ids == []

    def test_inhibit_marks_pipeline(self):
        self.inhibitor.inhibit("pipe-a", reason="maintenance", inhibited_by="ops")
        assert self.inhibitor.is_inhibited("pipe-a")

    def test_inhibit_returns_entry(self):
        entry = self.inhibitor.inhibit("pipe-b", reason="degraded", inhibited_by="auto")
        assert isinstance(entry, InhibitEntry)
        assert entry.pipeline_id == "pipe-b"
        assert entry.reason == "degraded"
        assert entry.inhibited_by == "auto"

    def test_release_removes_inhibition(self):
        self.inhibitor.inhibit("pipe-a", reason="test")
        released = self.inhibitor.release("pipe-a")
        assert released is True
        assert not self.inhibitor.is_inhibited("pipe-a")

    def test_release_returns_false_for_unknown_pipeline(self):
        result = self.inhibitor.release("nonexistent")
        assert result is False

    def test_is_inhibited_returns_false_when_not_set(self):
        assert not self.inhibitor.is_inhibited("pipe-x")

    def test_entry_for_returns_none_when_not_inhibited(self):
        assert self.inhibitor.entry_for("pipe-z") is None

    def test_entry_for_returns_correct_entry(self):
        self.inhibitor.inhibit("pipe-c", reason="quota", inhibited_by="scheduler")
        entry = self.inhibitor.entry_for("pipe-c")
        assert entry is not None
        assert entry.pipeline_id == "pipe-c"

    def test_multiple_pipelines_tracked_independently(self):
        self.inhibitor.inhibit("pipe-1", reason="r1")
        self.inhibitor.inhibit("pipe-2", reason="r2")
        assert len(self.inhibitor.inhibited_pipeline_ids) == 2
        assert self.inhibitor.is_inhibited("pipe-1")
        assert self.inhibitor.is_inhibited("pipe-2")

    def test_evaluate_returns_only_inhibited_snapshots(self):
        self.inhibitor.inhibit("pipe-a", reason="test")
        snapshots = [
            make_snapshot("pipe-a"),
            make_snapshot("pipe-b"),
        ]
        result = self.inhibitor.evaluate(snapshots)
        assert isinstance(result, InhibitorResult)
        assert len(result.entries) == 1
        assert result.entries[0].pipeline_id == "pipe-a"

    def test_evaluate_returns_empty_when_no_inhibitions(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.inhibitor.evaluate(snapshots)
        assert result.entries == []

    def test_inhibit_overwrites_existing_entry(self):
        self.inhibitor.inhibit("pipe-a", reason="first", inhibited_by="user1")
        self.inhibitor.inhibit("pipe-a", reason="second", inhibited_by="user2")
        entry = self.inhibitor.entry_for("pipe-a")
        assert entry.reason == "second"
        assert entry.inhibited_by == "user2"
