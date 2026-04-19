import pytest
from datetime import datetime, timedelta
from pipewatch.pipeline_quarantiner import PipelineQuarantiner, QuarantineEntry
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    metric = PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        records_processed=100,
        records_failed=0,
        duration_seconds=1.0,
    )
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metrics=[metric],
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
    )


class TestPipelineQuarantiner:
    def setup_method(self):
        self.quarantiner = PipelineQuarantiner()

    def test_new_quarantiner_has_no_entries(self):
        assert not self.quarantiner.is_quarantined("pipe-a")

    def test_quarantine_marks_pipeline(self):
        self.quarantiner.quarantine("pipe-a", reason="high error rate")
        assert self.quarantiner.is_quarantined("pipe-a")

    def test_release_removes_quarantine(self):
        self.quarantiner.quarantine("pipe-a", reason="test")
        self.quarantiner.release("pipe-a")
        assert not self.quarantiner.is_quarantined("pipe-a")

    def test_expired_entry_not_active(self):
        past = datetime.utcnow() - timedelta(seconds=600)
        self.quarantiner.quarantine("pipe-a", reason="old", duration_seconds=60.0, now=past)
        assert not self.quarantiner.is_quarantined("pipe-a")

    def test_active_entry_is_active(self):
        self.quarantiner.quarantine("pipe-a", reason="test", duration_seconds=300.0)
        assert self.quarantiner.is_quarantined("pipe-a")

    def test_run_separates_quarantined_and_allowed(self):
        self.quarantiner.quarantine("pipe-a", reason="bad")
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.quarantiner.run(snapshots)
        assert result.total_quarantined == 1
        assert "pipe-b" in result.allowed
        assert "pipe-a" not in result.allowed

    def test_run_empty_snapshots_returns_empty(self):
        result = self.quarantiner.run([])
        assert result.total_quarantined == 0
        assert result.total_allowed == 0

    def test_run_no_quarantines_all_allowed(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.quarantiner.run(snapshots)
        assert result.total_quarantined == 0
        assert result.total_allowed == 2

    def test_entry_str_shows_status(self):
        entry = self.quarantiner.quarantine("pipe-a", reason="spike")
        assert "active" in str(entry)
        assert "pipe-a" in str(entry)

    def test_pipeline_ids_in_result(self):
        self.quarantiner.quarantine("pipe-a", reason="test")
        snapshots = [make_snapshot("pipe-a")]
        result = self.quarantiner.run(snapshots)
        assert "pipe-a" in result.pipeline_ids
