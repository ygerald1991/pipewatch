import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_reaper import reap_snapshots, ReaperResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestReapSnapshots:
    def test_empty_returns_empty(self):
        result = reap_snapshots([])
        assert isinstance(result, ReaperResult)
        assert result.total_reaped == 0
        assert result.results == []

    def test_healthy_pipeline_kept(self):
        snap = make_snapshot("p1", error_rate=0.1)
        result = reap_snapshots([snap])
        assert result.total_reaped == 0
        assert len(result.kept) == 1

    def test_high_error_rate_reaped(self):
        snap = make_snapshot("p1", error_rate=0.9)
        result = reap_snapshots([snap], max_error_rate=0.75)
        assert result.total_reaped == 1
        assert result.reaped[0].pipeline_id == "p1"

    def test_exact_threshold_is_reaped(self):
        snap = make_snapshot("p1", error_rate=0.75)
        result = reap_snapshots([snap], max_error_rate=0.75)
        assert result.total_reaped == 1

    def test_below_threshold_kept(self):
        snap = make_snapshot("p1", error_rate=0.74)
        result = reap_snapshots([snap], max_error_rate=0.75)
        assert result.total_reaped == 0

    def test_low_throughput_reaped(self):
        snap = make_snapshot("p2", error_rate=0.0, throughput=5.0)
        result = reap_snapshots([snap], min_throughput=10.0)
        assert result.total_reaped == 1
        assert result.reaped[0].pipeline_id == "p2"

    def test_sufficient_throughput_kept(self):
        snap = make_snapshot("p2", error_rate=0.0, throughput=20.0)
        result = reap_snapshots([snap], min_throughput=10.0)
        assert result.total_reaped == 0

    def test_pipeline_ids_present(self):
        snaps = [make_snapshot("a"), make_snapshot("b")]
        result = reap_snapshots(snaps)
        assert set(result.pipeline_ids) == {"a", "b"}

    def test_reap_reason_contains_threshold(self):
        snap = make_snapshot("p1", error_rate=0.9)
        result = reap_snapshots([snap], max_error_rate=0.75)
        assert "0.75" in result.reaped[0].reason or "75%" in result.reaped[0].reason

    def test_str_shows_reaped_status(self):
        snap = make_snapshot("p1", error_rate=0.9)
        result = reap_snapshots([snap])
        assert "REAPED" in str(result.results[0])

    def test_str_shows_kept_status(self):
        snap = make_snapshot("p1", error_rate=0.1)
        result = reap_snapshots([snap])
        assert "KEPT" in str(result.results[0])
