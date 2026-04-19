from __future__ import annotations
from datetime import datetime
from unittest.mock import patch
from pipewatch.pipeline_activator import PipelineActivator, ActivatorResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
        metric_count=1,
        captured_at=datetime.utcnow(),
    )


class TestPipelineActivator:
    def setup_method(self):
        self.activator = PipelineActivator()

    def test_new_activator_has_no_active_pipelines(self):
        result = self.activator.run([])
        assert result.total_active == 0

    def test_activate_marks_pipeline_as_active(self):
        self.activator.activate("pipe-1")
        assert self.activator.is_active("pipe-1")

    def test_deactivate_removes_pipeline(self):
        self.activator.activate("pipe-1")
        self.activator.deactivate("pipe-1")
        assert not self.activator.is_active("pipe-1")

    def test_deactivate_unknown_pipeline_is_safe(self):
        self.activator.deactivate("ghost")
        assert not self.activator.is_active("ghost")

    def test_entry_for_returns_none_when_not_active(self):
        assert self.activator.entry_for("pipe-x") is None

    def test_entry_for_returns_entry_when_active(self):
        self.activator.activate("pipe-1", activated_by="user")
        entry = self.activator.entry_for("pipe-1")
        assert entry is not None
        assert entry.pipeline_id == "pipe-1"
        assert entry.activated_by == "user"

    def test_run_returns_only_active_snapshots(self):
        self.activator.activate("pipe-1")
        snapshots = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.activator.run(snapshots)
        assert result.total_active == 1
        assert "pipe-1" in result.pipeline_ids
        assert "pipe-2" not in result.pipeline_ids

    def test_run_returns_empty_when_no_snapshots_active(self):
        snapshots = [make_snapshot("pipe-1")]
        result = self.activator.run(snapshots)
        assert result.total_active == 0

    def test_is_active_false_for_unknown_pipeline(self):
        result = ActivatorResult(entries=[])
        assert not result.is_active("unknown")

    def test_activate_records_timestamp(self):
        fixed = datetime(2024, 1, 15, 12, 0, 0)
        with patch("pipewatch.pipeline_activator.datetime") as mock_dt:
            mock_dt.utcnow.return_value = fixed
            self.activator.activate("pipe-1")
        entry = self.activator.entry_for("pipe-1")
        assert entry.activated_at == fixed
