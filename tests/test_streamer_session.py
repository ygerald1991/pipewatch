import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.streamer_session import StreamerSession


def make_snapshot(pipeline_id: str, error_rate: float = 0.01) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=3,
        error_rate=error_rate,
        throughput=50.0,
        status=PipelineStatus.HEALTHY,
    )


class TestStreamerSession:
    def test_empty_session_returns_no_chunks(self):
        session = StreamerSession()
        result = session.run()
        assert result.total == 0

    def test_pipeline_ids_empty_initially(self):
        session = StreamerSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = StreamerSession()
        session.add_snapshot(make_snapshot("alpha"))
        assert "alpha" in session.pipeline_ids()

    def test_run_produces_chunk_per_snapshot(self):
        session = StreamerSession()
        session.add_snapshot(make_snapshot("a"))
        session.add_snapshot(make_snapshot("b"))
        result = session.run()
        assert result.total == 2

    def test_handler_called_for_each_chunk(self):
        session = StreamerSession()
        session.add_snapshot(make_snapshot("p1"))
        session.add_snapshot(make_snapshot("p2"))
        collected = []
        session.add_handler(lambda c: collected.append(c.pipeline_id))
        session.run()
        assert sorted(collected) == ["p1", "p2"]

    def test_multiple_handlers_all_called(self):
        session = StreamerSession()
        session.add_snapshot(make_snapshot("x"))
        log1, log2 = [], []
        session.add_handler(lambda c: log1.append(c.index))
        session.add_handler(lambda c: log2.append(c.index))
        session.run()
        assert log1 == [0]
        assert log2 == [0]
