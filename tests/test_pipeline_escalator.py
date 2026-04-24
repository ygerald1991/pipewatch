from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_escalator import (
    EscalatorResult,
    PipelineEscalator,
    EscalateEntry,
)
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


class TestPipelineEscalator:
    def setup_method(self):
        self.escalator = PipelineEscalator(max_level=3, level_window_seconds=300)

    def test_new_escalator_has_no_entries(self):
        assert self.escalator.current_level("pipe-1") == 0

    def test_escalate_sets_level_one(self):
        entry = self.escalator.escalate("pipe-1", reason="high error rate")
        assert entry.level == 1
        assert entry.pipeline_id == "pipe-1"
        assert entry.reason == "high error rate"

    def test_repeated_escalation_increases_level(self):
        self.escalator.escalate("pipe-1")
        self.escalator.escalate("pipe-1")
        entry = self.escalator.escalate("pipe-1")
        assert entry.level == 3

    def test_escalation_capped_at_max_level(self):
        for _ in range(10):
            entry = self.escalator.escalate("pipe-1")
        assert entry.level == 3

    def test_reset_clears_level(self):
        self.escalator.escalate("pipe-1")
        self.escalator.reset("pipe-1")
        assert self.escalator.current_level("pipe-1") == 0

    def test_expired_entry_returns_level_zero(self):
        past = datetime.utcnow() - timedelta(seconds=600)
        self.escalator.escalate("pipe-1", now=past)
        assert self.escalator.current_level("pipe-1") == 0

    def test_active_entry_returns_correct_level(self):
        self.escalator.escalate("pipe-1")
        self.escalator.escalate("pipe-1")
        assert self.escalator.current_level("pipe-1") == 2

    def test_run_returns_result_per_snapshot(self):
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        self.escalator.escalate("pipe-1")
        result = self.escalator.run(snaps)
        assert isinstance(result, EscalatorResult)
        assert len(result.entries) == 2

    def test_run_reflects_escalated_pipeline(self):
        self.escalator.escalate("pipe-1")
        result = self.escalator.run([make_snapshot("pipe-1")])
        entry = result.for_pipeline("pipe-1")
        assert entry is not None
        assert entry.level == 1

    def test_run_non_escalated_pipeline_has_level_zero(self):
        result = self.escalator.run([make_snapshot("pipe-2")])
        entry = result.for_pipeline("pipe-2")
        assert entry is not None
        assert entry.level == 0

    def test_total_escalated_counts_nonzero_levels(self):
        self.escalator.escalate("pipe-1")
        self.escalator.escalate("pipe-2")
        result = self.escalator.run(
            [make_snapshot("pipe-1"), make_snapshot("pipe-2"), make_snapshot("pipe-3")]
        )
        assert result.total_escalated == 2

    def test_at_level_filters_correctly(self):
        self.escalator.escalate("pipe-1")
        self.escalator.escalate("pipe-2")
        self.escalator.escalate("pipe-2")
        result = self.escalator.run(
            [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        )
        assert len(result.at_level(1)) == 1
        assert len(result.at_level(2)) == 1
