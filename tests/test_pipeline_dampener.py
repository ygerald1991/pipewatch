from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.pipeline_dampener import DampenEntry, DampenerResult, PipelineDampener
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str = "pipe-1") -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.01,
        throughput=100.0,
        metric_count=5,
        captured_at=datetime.utcnow(),
    )


class TestPipelineDampener:
    def setup_method(self):
        self.dampener = PipelineDampener(default_duration_seconds=30.0)

    def test_new_dampener_has_no_pipeline_ids(self):
        assert self.dampener.pipeline_ids == []

    def test_dampen_registers_pipeline(self):
        self.dampener.dampen("pipe-1", reason="high error rate")
        assert "pipe-1" in self.dampener.pipeline_ids

    def test_is_dampened_returns_true_for_active_entry(self):
        self.dampener.dampen("pipe-1", duration_seconds=60.0)
        assert self.dampener.is_dampened("pipe-1") is True

    def test_is_dampened_returns_false_for_unknown_pipeline(self):
        assert self.dampener.is_dampened("pipe-unknown") is False

    def test_is_dampened_returns_false_after_expiry(self):
        entry = self.dampener.dampen("pipe-1", duration_seconds=1.0)
        future = entry.expires_at() + timedelta(seconds=1)
        with patch("pipewatch.pipeline_dampener.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future
            assert entry.is_active() is False

    def test_release_removes_pipeline(self):
        self.dampener.dampen("pipe-1")
        result = self.dampener.release("pipe-1")
        assert result is True
        assert "pipe-1" not in self.dampener.pipeline_ids

    def test_release_unknown_pipeline_returns_false(self):
        result = self.dampener.release("pipe-missing")
        assert result is False

    def test_apply_returns_only_dampened_snapshots(self):
        self.dampener.dampen("pipe-1")
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.dampener.apply(snaps)
        assert isinstance(result, DampenerResult)
        assert len(result.entries) == 1
        assert result.entries[0].pipeline_id == "pipe-1"

    def test_apply_empty_snapshots_returns_empty_result(self):
        result = self.dampener.apply([])
        assert result.total_dampened == 0

    def test_total_dampened_counts_active_entries(self):
        self.dampener.dampen("pipe-1")
        self.dampener.dampen("pipe-2")
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.dampener.apply(snaps)
        assert result.total_dampened == 2

    def test_dampen_entry_str_contains_pipeline_id(self):
        entry = self.dampener.dampen("pipe-x", reason="test")
        assert "pipe-x" in str(entry)

    def test_default_duration_used_when_not_specified(self):
        entry = self.dampener.dampen("pipe-1")
        assert entry.duration_seconds == 30.0

    def test_custom_duration_overrides_default(self):
        entry = self.dampener.dampen("pipe-1", duration_seconds=120.0)
        assert entry.duration_seconds == 120.0
