import pytest
from datetime import datetime, timezone

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.quota_tracker import QuotaPolicy, QuotaResult, evaluate_quota


def make_metric(
    pipeline_id: str = "pipe-1",
    processed: int = 100,
    failed: int = 0,
    duration_seconds: float = 1.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(timezone.utc),
        processed=processed,
        failed=failed,
        duration_seconds=duration_seconds,
        status=PipelineStatus.HEALTHY,
    )


class TestQuotaPolicy:
    def test_default_max_throughput_is_sensible(self):
        policy = QuotaPolicy()
        assert policy.default_max_throughput > 0

    def test_max_throughput_for_returns_default_when_no_override(self):
        policy = QuotaPolicy(default_max_throughput=5000.0)
        assert policy.max_throughput_for("unknown-pipe") == 5000.0

    def test_max_throughput_for_returns_override_when_present(self):
        policy = QuotaPolicy(
            default_max_throughput=5000.0,
            overrides={"pipe-x": 1000.0},
        )
        assert policy.max_throughput_for("pipe-x") == 1000.0

    def test_override_does_not_affect_other_pipelines(self):
        policy = QuotaPolicy(
            default_max_throughput=5000.0,
            overrides={"pipe-x": 1000.0},
        )
        assert policy.max_throughput_for("pipe-y") == 5000.0


class TestEvaluateQuota:
    def test_returns_none_for_empty_metrics(self):
        policy = QuotaPolicy()
        result = evaluate_quota("pipe-1", [], policy)
        assert result is None

    def test_not_exceeded_when_below_limit(self):
        metrics = [make_metric(processed=50, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=1000.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert not result.exceeded

    def test_exceeded_when_above_limit(self):
        metrics = [make_metric(processed=500, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=100.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert result.exceeded

    def test_utilization_is_correct(self):
        metrics = [make_metric(processed=200, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=400.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert abs(result.utilization - 0.5) < 1e-6

    def test_utilization_capped_logic_above_one_when_exceeded(self):
        metrics = [make_metric(processed=600, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=100.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert result.utilization > 1.0

    def test_pipeline_id_in_result(self):
        metrics = [make_metric(pipeline_id="my-pipe", processed=10)]
        policy = QuotaPolicy()
        result = evaluate_quota("my-pipe", metrics, policy)
        assert result is not None
        assert result.pipeline_id == "my-pipe"

    def test_str_contains_pipeline_id(self):
        metrics = [make_metric(processed=10, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=1000.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert "pipe-1" in str(result)

    def test_str_shows_exceeded_status(self):
        metrics = [make_metric(processed=9999, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=10.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert "EXCEEDED" in str(result)

    def test_str_shows_ok_status(self):
        metrics = [make_metric(processed=1, duration_seconds=1.0)]
        policy = QuotaPolicy(default_max_throughput=1000.0)
        result = evaluate_quota("pipe-1", metrics, policy)
        assert result is not None
        assert "OK" in str(result)
