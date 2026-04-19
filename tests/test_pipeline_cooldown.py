from datetime import datetime, timedelta
import pytest
from pipewatch.pipeline_cooldown import PipelineCooldown, CooldownEntry, CooldownResult
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


class TestPipelineCooldown:
    def setup_method(self):
        self.cooldown = PipelineCooldown()

    def test_new_cooldown_has_no_entries(self):
        assert self.cooldown.pipeline_ids == []

    def test_cool_registers_pipeline(self):
        self.cooldown.cool("pipe-1", 60.0, reason="high error rate")
        assert "pipe-1" in self.cooldown.pipeline_ids

    def test_is_cooling_returns_true_for_active(self):
        self.cooldown.cool("pipe-1", 3600.0)
        assert self.cooldown.is_cooling("pipe-1") is True

    def test_is_cooling_returns_false_for_unknown(self):
        assert self.cooldown.is_cooling("unknown") is False

    def test_is_cooling_returns_false_after_expiry(self):
        self.cooldown.cool("pipe-1", 1.0)
        future = datetime.utcnow() + timedelta(seconds=10)
        assert self.cooldown.is_cooling("pipe-1", now=future) is False

    def test_release_removes_entry(self):
        self.cooldown.cool("pipe-1", 60.0)
        self.cooldown.release("pipe-1")
        assert self.cooldown.is_cooling("pipe-1") is False

    def test_release_unknown_pipeline_does_not_raise(self):
        self.cooldown.release("nonexistent")

    def test_evaluate_excludes_cooling_snapshots(self):
        self.cooldown.cool("pipe-1", 3600.0)
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.cooldown.evaluate(snaps)
        assert len(result.snapshots) == 1
        assert result.snapshots[0].pipeline_id == "pipe-2"

    def test_evaluate_allows_all_when_none_cooling(self):
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.cooldown.evaluate(snaps)
        assert len(result.snapshots) == 2

    def test_total_cooling_counts_active_entries(self):
        self.cooldown.cool("pipe-1", 3600.0)
        self.cooldown.cool("pipe-2", 3600.0)
        result = self.cooldown.evaluate([])
        assert result.total_cooling == 2

    def test_entry_str_contains_pipeline_id(self):
        entry = self.cooldown.cool("pipe-x", 30.0, reason="test")
        assert "pipe-x" in str(entry)

    def test_entry_expires_at_is_correct(self):
        before = datetime.utcnow()
        entry = self.cooldown.cool("pipe-1", 120.0)
        after = datetime.utcnow()
        assert before + timedelta(seconds=120) <= entry.expires_at() <= after + timedelta(seconds=120)
