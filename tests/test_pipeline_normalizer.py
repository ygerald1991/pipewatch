import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_normalizer import normalize_snapshots, NormalizerResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestNormalizeSnapshots:
    def test_empty_returns_empty(self):
        result = normalize_snapshots([])
        assert isinstance(result, NormalizerResult)
        assert result.results == []

    def test_single_snapshot_error_rate_is_zero(self):
        result = normalize_snapshots([make_snapshot("p1", error_rate=0.5)])
        r = result.for_pipeline("p1")
        assert r is not None
        assert r.normalized_error_rate == 0.0

    def test_single_snapshot_throughput_is_zero(self):
        result = normalize_snapshots([make_snapshot("p1", throughput=200.0)])
        r = result.for_pipeline("p1")
        assert r.normalized_throughput == 0.0

    def test_two_snapshots_min_error_rate_is_zero(self):
        snaps = [make_snapshot("p1", error_rate=0.1), make_snapshot("p2", error_rate=0.5)]
        result = normalize_snapshots(snaps)
        assert result.for_pipeline("p1").normalized_error_rate == pytest.approx(0.0)

    def test_two_snapshots_max_error_rate_is_one(self):
        snaps = [make_snapshot("p1", error_rate=0.1), make_snapshot("p2", error_rate=0.5)]
        result = normalize_snapshots(snaps)
        assert result.for_pipeline("p2").normalized_error_rate == pytest.approx(1.0)

    def test_normalized_throughput_range(self):
        snaps = [make_snapshot("p1", throughput=0.0), make_snapshot("p2", throughput=100.0)]
        result = normalize_snapshots(snaps)
        assert result.for_pipeline("p1").normalized_throughput == pytest.approx(0.0)
        assert result.for_pipeline("p2").normalized_throughput == pytest.approx(1.0)

    def test_pipeline_ids_preserved(self):
        snaps = [make_snapshot("alpha"), make_snapshot("beta")]
        result = normalize_snapshots(snaps)
        assert set(result.pipeline_ids()) == {"alpha", "beta"}

    def test_for_pipeline_returns_none_for_unknown(self):
        result = normalize_snapshots([make_snapshot("p1")])
        assert result.for_pipeline("unknown") is None

    def test_original_values_preserved(self):
        snap = make_snapshot("p1", error_rate=0.3, throughput=42.0)
        result = normalize_snapshots([snap])
        r = result.for_pipeline("p1")
        assert r.original_error_rate == pytest.approx(0.3)
        assert r.original_throughput == pytest.approx(42.0)

    def test_midpoint_normalizes_correctly(self):
        snaps = [
            make_snapshot("low", error_rate=0.0),
            make_snapshot("mid", error_rate=0.5),
            make_snapshot("high", error_rate=1.0),
        ]
        result = normalize_snapshots(snaps)
        assert result.for_pipeline("mid").normalized_error_rate == pytest.approx(0.5)
