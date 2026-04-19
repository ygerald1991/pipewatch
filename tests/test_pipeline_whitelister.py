import pytest
from pipewatch.pipeline_whitelister import PipelineWhitelister, WhitelistEntry, WhitelisterResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        avg_error_rate=error_rate,
        avg_throughput=100.0,
        status=PipelineStatus.HEALTHY,
    )


class TestPipelineWhitelister:
    def setup_method(self):
        self.whitelister = PipelineWhitelister()

    def test_new_whitelister_has_no_entries(self):
        assert self.whitelister.whitelisted_ids == []

    def test_whitelist_marks_pipeline(self):
        self.whitelister.whitelist("pipe-a", reason="trusted")
        assert self.whitelister.is_whitelisted("pipe-a")

    def test_unwhitelisted_pipeline_not_allowed(self):
        assert not self.whitelister.is_whitelisted("pipe-x")

    def test_unwhitelist_removes_pipeline(self):
        self.whitelister.whitelist("pipe-a")
        self.whitelister.unwhitelist("pipe-a")
        assert not self.whitelister.is_whitelisted("pipe-a")

    def test_run_allows_whitelisted_pipeline(self):
        self.whitelister.whitelist("pipe-a")
        snapshots = [make_snapshot("pipe-a")]
        result = self.whitelister.run(snapshots)
        assert result.total_allowed == 1
        assert result.total_blocked == 0

    def test_run_blocks_non_whitelisted_pipeline(self):
        snapshots = [make_snapshot("pipe-b")]
        result = self.whitelister.run(snapshots)
        assert result.total_blocked == 1
        assert result.total_allowed == 0

    def test_run_with_default_allow_true(self):
        snapshots = [make_snapshot("pipe-c")]
        result = self.whitelister.run(snapshots, default_allow=True)
        assert result.total_allowed == 1

    def test_run_mixed_pipelines(self):
        self.whitelister.whitelist("pipe-a")
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.whitelister.run(snapshots)
        assert result.total_allowed == 1
        assert result.total_blocked == 1

    def test_whitelist_entry_reason_stored(self):
        entry = self.whitelister.whitelist("pipe-a", reason="partner pipeline")
        assert entry.reason == "partner pipeline"

    def test_pipeline_ids_in_result(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.whitelister.run(snapshots)
        assert "pipe-a" in result.pipeline_ids
        assert "pipe-b" in result.pipeline_ids
