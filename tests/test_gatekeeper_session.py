import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.gatekeeper_session import GatekeeperSession


def make_snapshot(
    pipeline_id: str = "pipe-1",
    error_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=1,
        status="healthy",
    )


class TestGatekeeperSession:
    def setup_method(self):
        self.session = GatekeeperSession(max_error_rate=0.3, min_throughput=5.0)

    def test_empty_session_returns_none(self):
        assert self.session.run() is None

    def test_pipeline_ids_empty_initially(self):
        assert self.session.pipeline_ids == []

    def test_add_snapshot_registers_pipeline(self):
        self.session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in self.session.pipeline_ids

    def test_run_returns_result_for_added_snapshots(self):
        self.session.add_snapshot(make_snapshot("pipe-a", error_rate=0.05))
        result = self.session.run()
        assert result is not None
        assert "pipe-a" in result.pipeline_ids

    def test_override_allows_bad_pipeline(self):
        self.session.add_snapshot(make_snapshot("bad", error_rate=0.99))
        self.session.set_override("bad", True)
        result = self.session.run()
        assert result is not None
        assert "bad" in result.allowed_pipelines()

    def test_duplicate_add_uses_latest_snapshot(self):
        self.session.add_snapshot(make_snapshot("pipe-x", error_rate=0.9))
        self.session.add_snapshot(make_snapshot("pipe-x", error_rate=0.01))
        result = self.session.run()
        assert result is not None
        assert "pipe-x" in result.allowed_pipelines()
