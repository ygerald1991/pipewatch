import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_resetter import reset_snapshots, ResetterResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestResetSnapshots:
    def test_empty_returns_empty(self):
        result = reset_snapshots([])
        assert result.total_reset == 0
        assert result.pipeline_ids == []

    def test_single_snapshot_reset(self):
        snap = make_snapshot("pipe-1", error_rate=0.05)
        result = reset_snapshots([snap])
        assert result.total_reset == 1
        assert "pipe-1" in result.pipeline_ids

    def test_entry_stores_previous_values(self):
        snap = make_snapshot("pipe-1", error_rate=0.15, throughput=200.0)
        result = reset_snapshots([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry is not None
        assert entry.previous_error_rate == pytest.approx(0.15)
        assert entry.previous_throughput == pytest.approx(200.0)

    def test_reason_is_stored(self):
        snap = make_snapshot("pipe-1")
        result = reset_snapshots([snap], reason="scheduled_reset")
        entry = result.for_pipeline("pipe-1")
        assert entry.reason == "scheduled_reset"

    def test_default_reason_is_manual_reset(self):
        snap = make_snapshot("pipe-1")
        result = reset_snapshots([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry.reason == "manual_reset"

    def test_only_degraded_skips_healthy(self):
        healthy = make_snapshot("pipe-healthy", error_rate=0.01)
        degraded = make_snapshot("pipe-degraded", error_rate=0.25)
        result = reset_snapshots([healthy, degraded], only_degraded=True)
        assert result.total_reset == 1
        assert "pipe-degraded" in result.pipeline_ids
        assert "pipe-healthy" not in result.pipeline_ids

    def test_only_degraded_false_resets_all(self):
        snaps = [make_snapshot(f"pipe-{i}", error_rate=0.01 * i) for i in range(4)]
        result = reset_snapshots(snaps, only_degraded=False)
        assert result.total_reset == 4

    def test_for_pipeline_returns_none_for_unknown(self):
        result = ResetterResult()
        assert result.for_pipeline("nonexistent") is None

    def test_multiple_pipelines_all_recorded(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(3)]
        result = reset_snapshots(snaps)
        assert set(result.pipeline_ids) == {"pipe-0", "pipe-1", "pipe-2"}
