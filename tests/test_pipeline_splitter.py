import pytest
from pipewatch.pipeline_splitter import split_snapshots, SplitterResult, SplitResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        avg_error_rate=error_rate,
        avg_throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestSplitSnapshots:
    def test_empty_returns_empty(self):
        result = split_snapshots([], key_fn=lambda s: s.pipeline_id)
        assert isinstance(result, SplitterResult)
        assert result.total == 0
        assert result.bucket_names() == []

    def test_single_snapshot_creates_one_bucket(self):
        s = make_snapshot("pipe-a")
        result = split_snapshots([s], key_fn=lambda s: s.pipeline_id)
        assert "pipe-a" in result.bucket_names()
        assert result.total == 1

    def test_multiple_snapshots_same_bucket(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-a")]
        result = split_snapshots(snapshots, key_fn=lambda s: s.pipeline_id)
        assert len(result.bucket_names()) == 1
        assert result.for_bucket("pipe-a").size == 2

    def test_multiple_buckets_created(self):
        snapshots = [make_snapshot("pipe-a"), make_snapshot("pipe-b"), make_snapshot("pipe-a")]
        result = split_snapshots(snapshots, key_fn=lambda s: s.pipeline_id)
        assert set(result.bucket_names()) == {"pipe-a", "pipe-b"}
        assert result.for_bucket("pipe-a").size == 2
        assert result.for_bucket("pipe-b").size == 1

    def test_key_fn_by_error_band(self):
        low = make_snapshot("p1", error_rate=0.01)
        high = make_snapshot("p2", error_rate=0.25)
        band = lambda s: "high" if s.avg_error_rate >= 0.1 else "low"
        result = split_snapshots([low, high], key_fn=band)
        assert "low" in result.bucket_names()
        assert "high" in result.bucket_names()

    def test_for_bucket_returns_none_for_missing(self):
        result = split_snapshots([], key_fn=lambda s: s.pipeline_id)
        assert result.for_bucket("nonexistent") is None

    def test_pipeline_ids_in_bucket(self):
        snapshots = [make_snapshot("p1"), make_snapshot("p2")]
        result = split_snapshots(snapshots, key_fn=lambda _: "all")
        bucket = result.for_bucket("all")
        assert set(bucket.pipeline_ids) == {"p1", "p2"}

    def test_exception_in_key_fn_uses_default_bucket(self):
        s = make_snapshot("pipe-x")
        def bad_key(snap):
            raise ValueError("oops")
        result = split_snapshots([s], key_fn=bad_key, default_bucket="fallback")
        assert "fallback" in result.bucket_names()
        assert result.for_bucket("fallback").size == 1

    def test_str_representation(self):
        s = make_snapshot("pipe-a")
        result = split_snapshots([s], key_fn=lambda s: "grp")
        bucket = result.for_bucket("grp")
        assert "grp" in str(bucket)
        assert "1" in str(bucket)
