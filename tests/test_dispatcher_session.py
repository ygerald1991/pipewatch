import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_dispatcher import DispatchRule
from pipewatch.dispatcher_session import DispatcherSession


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=3,
    )


class TestDispatcherSession:
    def test_empty_session_returns_no_results(self):
        session = DispatcherSession()
        result = session.run()
        assert result.results == []

    def test_pipeline_ids_empty_initially(self):
        session = DispatcherSession()
        assert session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        session = DispatcherSession()
        session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in session.pipeline_ids()

    def test_run_dispatches_with_rule(self):
        session = DispatcherSession(default_target="default")
        rule = DispatchRule("high", lambda s: s.error_rate > 0.2, "alert")
        session.add_rule(rule)
        session.add_snapshot(make_snapshot("pipe-a", error_rate=0.5))
        session.add_snapshot(make_snapshot("pipe-b", error_rate=0.0))
        result = session.run()
        targets = {r.pipeline_id: r.target for r in result.results}
        assert targets["pipe-a"] == "alert"
        assert targets["pipe-b"] == "default"

    def test_no_rules_uses_default_target(self):
        session = DispatcherSession(default_target="fallback")
        session.add_snapshot(make_snapshot("pipe-x"))
        result = session.run()
        assert result.results[0].target == "fallback"

    def test_duplicate_snapshot_overrides_previous(self):
        session = DispatcherSession()
        session.add_snapshot(make_snapshot("pipe-a", error_rate=0.1))
        session.add_snapshot(make_snapshot("pipe-a", error_rate=0.9))
        assert len(session.pipeline_ids()) == 1
