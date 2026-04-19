import pytest
from datetime import datetime
from unittest.mock import patch

from pipewatch.pipeline_acknowledger import PipelineAcknowledger, AcknowledgerResult
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


class TestPipelineAcknowledger:
    def setup_method(self):
        self.acker = PipelineAcknowledger()

    def test_new_acknowledger_has_no_entries(self):
        assert self.acker.acknowledged_pipeline_ids == []

    def test_acknowledge_marks_pipeline(self):
        self.acker.acknowledge("pipe-1", "alice")
        assert self.acker.is_acknowledged("pipe-1")

    def test_acknowledge_returns_entry(self):
        entry = self.acker.acknowledge("pipe-1", "alice", reason="known issue")
        assert entry.pipeline_id == "pipe-1"
        assert entry.acknowledged_by == "alice"
        assert entry.reason == "known issue"

    def test_unacknowledge_removes_entry(self):
        self.acker.acknowledge("pipe-1", "alice")
        result = self.acker.unacknowledge("pipe-1")
        assert result is True
        assert not self.acker.is_acknowledged("pipe-1")

    def test_unacknowledge_unknown_returns_false(self):
        assert self.acker.unacknowledge("pipe-x") is False

    def test_get_returns_none_for_unknown(self):
        assert self.acker.get("pipe-x") is None

    def test_get_returns_entry_when_set(self):
        self.acker.acknowledge("pipe-1", "bob")
        entry = self.acker.get("pipe-1")
        assert entry is not None
        assert entry.acknowledged_by == "bob"

    def test_run_returns_only_acknowledged_snapshots(self):
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        self.acker.acknowledge("pipe-1", "alice")
        result = self.acker.run(snaps)
        assert result.total_acknowledged == 1
        assert "pipe-1" in result.pipeline_ids
        assert "pipe-2" not in result.pipeline_ids

    def test_run_empty_snapshots_returns_empty(self):
        self.acker.acknowledge("pipe-1", "alice")
        result = self.acker.run([])
        assert result.total_acknowledged == 0

    def test_for_pipeline_returns_correct_entry(self):
        snaps = [make_snapshot("pipe-1")]
        self.acker.acknowledge("pipe-1", "carol", reason="maintenance")
        result = self.acker.run(snaps)
        entry = result.for_pipeline("pipe-1")
        assert entry is not None
        assert entry.reason == "maintenance"

    def test_str_contains_pipeline_id(self):
        entry = self.acker.acknowledge("pipe-1", "dave")
        assert "pipe-1" in str(entry)
        assert "dave" in str(entry)
