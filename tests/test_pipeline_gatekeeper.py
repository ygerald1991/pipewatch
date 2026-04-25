import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_gatekeeper import PipelineGatekeeper, GatekeeperResult


def make_snapshot(
    pipeline_id: str = "pipe-1",
    error_rate: float = 0.0,
    throughput: float = 100.0,
    metric_count: int = 1,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=metric_count,
        status="healthy",
    )


class TestPipelineGatekeeper:
    def setup_method(self):
        self.gatekeeper = PipelineGatekeeper(max_error_rate=0.2, min_throughput=10.0)

    def test_healthy_pipeline_is_allowed(self):
        snap = make_snapshot(error_rate=0.05, throughput=50.0)
        result = self.gatekeeper.run([snap])
        assert result.total_allowed == 1
        assert result.total_blocked == 0

    def test_high_error_rate_is_blocked(self):
        snap = make_snapshot(error_rate=0.5, throughput=50.0)
        result = self.gatekeeper.run([snap])
        assert result.total_blocked == 1
        assert "error_rate" in result.entries[0].reason

    def test_low_throughput_is_blocked(self):
        snap = make_snapshot(error_rate=0.01, throughput=5.0)
        result = self.gatekeeper.run([snap])
        assert result.total_blocked == 1
        assert "throughput" in result.entries[0].reason

    def test_error_rate_at_threshold_is_blocked(self):
        snap = make_snapshot(error_rate=0.2, throughput=50.0)
        result = self.gatekeeper.run([snap])
        # strictly greater than, so exactly at threshold is blocked
        assert result.total_blocked == 1

    def test_error_rate_just_below_threshold_is_allowed(self):
        snap = make_snapshot(error_rate=0.19, throughput=50.0)
        result = self.gatekeeper.run([snap])
        assert result.total_allowed == 1

    def test_manual_override_allows_blocked_pipeline(self):
        gk = PipelineGatekeeper(
            max_error_rate=0.1, overrides={"pipe-bad": True}
        )
        snap = make_snapshot(pipeline_id="pipe-bad", error_rate=0.9)
        result = gk.run([snap])
        assert result.total_allowed == 1
        assert result.entries[0].reason == "manual override"

    def test_manual_override_blocks_healthy_pipeline(self):
        gk = PipelineGatekeeper(overrides={"pipe-ok": False})
        snap = make_snapshot(pipeline_id="pipe-ok", error_rate=0.0)
        result = gk.run([snap])
        assert result.total_blocked == 1

    def test_empty_snapshots_returns_empty_result(self):
        result = self.gatekeeper.run([])
        assert result.total_allowed == 0
        assert result.total_blocked == 0
        assert result.pipeline_ids == []

    def test_multiple_pipelines_mixed_result(self):
        snaps = [
            make_snapshot(pipeline_id="good", error_rate=0.01, throughput=50.0),
            make_snapshot(pipeline_id="bad", error_rate=0.99, throughput=50.0),
        ]
        result = self.gatekeeper.run(snaps)
        assert result.total_allowed == 1
        assert result.total_blocked == 1
        assert "good" in result.allowed_pipelines()
        assert "bad" in result.blocked_pipelines()
