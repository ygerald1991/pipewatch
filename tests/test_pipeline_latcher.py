import pytest
from datetime import datetime
from pipewatch.pipeline_latcher import PipelineLatcher, LatcherResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=5,
        captured_at=datetime.utcnow(),
    )


class TestPipelineLatcher:
    def setup_method(self):
        self.latcher = PipelineLatcher()

    def test_new_latcher_has_no_latched_pipelines(self):
        assert self.latcher.latched_pipeline_ids == []

    def test_latch_marks_pipeline(self):
        self.latcher.latch("pipe-a", reason="manual hold")
        assert self.latcher.is_latched("pipe-a")

    def test_latch_returns_entry(self):
        entry = self.latcher.latch("pipe-b", reason="degraded")
        assert entry.pipeline_id == "pipe-b"
        assert entry.reason == "degraded"

    def test_latch_stores_latched_by(self):
        entry = self.latcher.latch("pipe-c", reason="test", latched_by="ops-team")
        assert entry.latched_by == "ops-team"

    def test_unlatch_removes_pipeline(self):
        self.latcher.latch("pipe-a", reason="hold")
        result = self.latcher.unlatch("pipe-a")
        assert result is True
        assert not self.latcher.is_latched("pipe-a")

    def test_unlatch_returns_false_for_unknown(self):
        assert self.latcher.unlatch("nonexistent") is False

    def test_is_latched_false_when_not_set(self):
        assert not self.latcher.is_latched("pipe-z")

    def test_evaluate_returns_only_latched(self):
        self.latcher.latch("pipe-a", reason="hold")
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.latcher.evaluate(snaps)
        assert result.total_latched == 1
        assert "pipe-a" in result.pipeline_ids
        assert "pipe-b" not in result.pipeline_ids

    def test_evaluate_empty_when_none_latched(self):
        snaps = [make_snapshot("pipe-a")]
        result = self.latcher.evaluate(snaps)
        assert result.total_latched == 0

    def test_for_pipeline_returns_entry(self):
        self.latcher.latch("pipe-a", reason="degraded")
        snaps = [make_snapshot("pipe-a")]
        result = self.latcher.evaluate(snaps)
        entry = result.for_pipeline("pipe-a")
        assert entry is not None
        assert entry.reason == "degraded"

    def test_for_pipeline_returns_none_when_not_latched(self):
        result = LatcherResult(entries=[])
        assert result.for_pipeline("pipe-x") is None
