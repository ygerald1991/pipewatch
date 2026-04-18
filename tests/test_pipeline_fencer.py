from __future__ import annotations
import pytest
from datetime import datetime, timezone, timedelta
from pipewatch.pipeline_fencer import PipelineFencer, FenceEntry, FencerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=5,
    )


class TestPipelineFencer:
    def setup_method(self):
        self.fencer = PipelineFencer()

    def test_new_fencer_has_no_fenced_pipelines(self):
        assert self.fencer.fenced_pipelines() == []

    def test_fence_marks_pipeline_as_fenced(self):
        self.fencer.fence("pipe-1", reason="high error rate")
        assert self.fencer.is_fenced("pipe-1")

    def test_unfenced_pipeline_is_not_fenced(self):
        assert not self.fencer.is_fenced("pipe-x")

    def test_unfence_removes_entry(self):
        self.fencer.fence("pipe-1", reason="test")
        removed = self.fencer.unfence("pipe-1")
        assert removed is True
        assert not self.fencer.is_fenced("pipe-1")

    def test_unfence_returns_false_for_unknown(self):
        assert self.fencer.unfence("nonexistent") is False

    def test_expired_fence_treated_as_not_fenced(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        self.fencer.fence("pipe-1", reason="expired", expires_at=past)
        assert not self.fencer.is_fenced("pipe-1")

    def test_active_expiry_fence_is_fenced(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        self.fencer.fence("pipe-1", reason="temp", expires_at=future)
        assert self.fencer.is_fenced("pipe-1")

    def test_evaluate_separates_fenced_and_allowed(self):
        self.fencer.fence("pipe-1", reason="bad")
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.fencer.evaluate(snaps)
        assert "pipe-1" in result.fenced
        assert "pipe-2" in result.allowed

    def test_evaluate_empty_snapshots_returns_empty_result(self):
        result = self.fencer.evaluate([])
        assert result.total_fenced == 0
        assert result.total_allowed == 0

    def test_fenced_pipelines_returns_all_active(self):
        self.fencer.fence("pipe-a", reason="r1")
        self.fencer.fence("pipe-b", reason="r2")
        active = self.fencer.fenced_pipelines()
        assert set(active) == {"pipe-a", "pipe-b"}

    def test_fence_entry_str(self):
        entry = FenceEntry(pipeline_id="pipe-1", reason="test")
        assert "pipe-1" in str(entry)
        assert "test" in str(entry)
