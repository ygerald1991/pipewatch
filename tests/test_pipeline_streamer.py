import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_streamer import (
    StreamChunk,
    StreamResult,
    stream_snapshots,
    collect_stream,
)


def make_snapshot(pipeline_id: str, error_rate: float = 0.01, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestStreamSnapshots:
    def test_empty_returns_no_chunks(self):
        result = collect_stream([])
        assert result.total == 0

    def test_single_snapshot_produces_one_chunk(self):
        s = make_snapshot("pipe-a")
        result = collect_stream([s])
        assert result.total == 1

    def test_chunk_index_increments(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(3)]
        result = collect_stream(snaps)
        indices = [c.index for c in result.chunks]
        assert indices == [0, 1, 2]

    def test_chunk_pipeline_id_matches_snapshot(self):
        s = make_snapshot("my-pipeline")
        result = collect_stream([s])
        assert result.chunks[0].pipeline_id == "my-pipeline"

    def test_pipeline_ids_returns_unique(self):
        snaps = [make_snapshot("a"), make_snapshot("a"), make_snapshot("b")]
        result = collect_stream(snaps)
        assert sorted(result.pipeline_ids()) == ["a", "b"]

    def test_for_pipeline_filters_correctly(self):
        snaps = [make_snapshot("a"), make_snapshot("b"), make_snapshot("a")]
        result = collect_stream(snaps)
        a_chunks = result.for_pipeline("a")
        assert len(a_chunks) == 2
        assert all(c.pipeline_id == "a" for c in a_chunks)

    def test_on_chunk_callback_called_for_each(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(4)]
        seen = []
        collect_stream(snaps, on_chunk=lambda c: seen.append(c.index))
        assert seen == [0, 1, 2, 3]

    def test_stream_snapshots_is_lazy_iterator(self):
        snaps = [make_snapshot("x"), make_snapshot("y")]
        gen = stream_snapshots(snaps)
        first = next(gen)
        assert isinstance(first, StreamChunk)
        assert first.index == 0

    def test_chunk_str_contains_pipeline_id(self):
        s = make_snapshot("trace-pipe")
        result = collect_stream([s])
        assert "trace-pipe" in str(result.chunks[0])
