import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_scorer_session import ScorerSession


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=10,
    )


class TestScorerSession:
    def test_empty_session_returns_none(self):
        session = ScorerSession()
        assert session.run() is None

    def test_pipeline_ids_empty_initially(self):
        session = ScorerSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = ScorerSession()
        session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in session.pipeline_ids()

    def test_run_returns_scoring_result(self):
        session = ScorerSession()
        session.add_snapshot(make_snapshot("pipe-a"))
        session.add_snapshot(make_snapshot("pipe-b", error_rate=0.5))
        result = session.run()
        assert result is not None
        assert len(result.scores) == 2

    def test_top_returns_limited_entries(self):
        session = ScorerSession()
        for i in range(5):
            session.add_snapshot(make_snapshot(f"pipe-{i}", error_rate=i * 0.05))
        top = session.top(2)
        assert len(top) == 2

    def test_bottom_returns_lowest_scores(self):
        session = ScorerSession()
        session.add_snapshot(make_snapshot("good", error_rate=0.0))
        session.add_snapshot(make_snapshot("bad", error_rate=0.9))
        bottom = session.bottom(1)
        assert bottom[0].pipeline_id == "bad"

    def test_top_empty_session_returns_empty(self):
        session = ScorerSession()
        assert session.top(3) == []

    def test_bottom_empty_session_returns_empty(self):
        session = ScorerSession()
        assert session.bottom(3) == []
