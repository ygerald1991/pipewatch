from datetime import datetime, timedelta
import pytest
from pipewatch.pipeline_debouncer import PipelineDebouncer, DebouncerResult
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str = "pipe-1") -> PipelineSnapshot:
    return PipelineSnapshot(pipeline_id=pipeline_id, metrics=[], timestamp=datetime.utcnow())


class TestPipelineDebouncer:
    def setup_method(self):
        self.debouncer = PipelineDebouncer(window_seconds=30.0, min_triggers=3)

    def test_new_debouncer_has_no_active_pipelines(self):
        assert self.debouncer.active_pipeline_ids == []

    def test_first_trigger_does_not_fire(self):
        entry = self.debouncer.trigger("pipe-1")
        assert not entry.fired
        assert entry.trigger_count == 1

    def test_below_min_triggers_does_not_fire(self):
        self.debouncer.trigger("pipe-1")
        entry = self.debouncer.trigger("pipe-1")
        assert not entry.fired
        assert entry.trigger_count == 2

    def test_fires_at_min_triggers_within_window(self):
        now = datetime.utcnow()
        self.debouncer.trigger("pipe-1", now=now)
        self.debouncer.trigger("pipe-1", now=now + timedelta(seconds=5))
        entry = self.debouncer.trigger("pipe-1", now=now + timedelta(seconds=10))
        assert entry.fired
        assert entry.trigger_count == 3

    def test_does_not_fire_when_outside_window(self):
        now = datetime.utcnow()
        self.debouncer.trigger("pipe-1", now=now)
        self.debouncer.trigger("pipe-1", now=now + timedelta(seconds=20))
        entry = self.debouncer.trigger("pipe-1", now=now + timedelta(seconds=60))
        assert not entry.fired

    def test_reset_clears_pipeline_state(self):
        self.debouncer.trigger("pipe-1")
        self.debouncer.reset("pipe-1")
        assert "pipe-1" not in self.debouncer.active_pipeline_ids

    def test_reset_unknown_pipeline_does_not_raise(self):
        self.debouncer.reset("nonexistent")

    def test_run_returns_debouncer_result(self):
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.debouncer.run(snapshots)
        assert isinstance(result, DebouncerResult)
        assert len(result.entries) == 2

    def test_run_registers_pipeline_ids(self):
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.debouncer.run(snapshots)
        assert "pipe-1" in result.pipeline_ids
        assert "pipe-2" in result.pipeline_ids

    def test_total_fired_counts_only_fired(self):
        now = datetime.utcnow()
        for i in range(3):
            self.debouncer.trigger("pipe-1", now=now + timedelta(seconds=i))
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.debouncer.run(snapshots)
        assert result.total_fired >= 1

    def test_total_pending_counts_unfired(self):
        snapshots = [make_snapshot("pipe-fresh")]
        result = self.debouncer.run(snapshots)
        assert result.total_pending >= 1
