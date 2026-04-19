import pytest
from pipewatch.circuit_breaker_session import CircuitBreakerSession
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, error_rate: float) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=100.0,
        status="healthy" if error_rate < 0.1 else "critical",
    )


class TestCircuitBreakerSession:
    def setup_method(self):
        self.session = CircuitBreakerSession(threshold=3, cooldown_seconds=60, error_threshold=0.1)

    def test_empty_session_returns_no_results(self):
        assert self.session.run() == {}

    def test_pipeline_ids_empty_initially(self):
        assert self.session.pipeline_ids() == []

    def test_add_snapshot_registers_pipeline(self):
        self.session.add_snapshot(make_snapshot("pipe-a", 0.0))
        assert "pipe-a" in self.session.pipeline_ids()

    def test_healthy_snapshot_not_tripped(self):
        self.session.add_snapshot(make_snapshot("pipe-a", 0.01))
        results = self.session.run()
        assert not results["pipe-a"].tripped

    def test_repeated_failures_trip_circuit(self):
        session = CircuitBreakerSession(threshold=2, error_threshold=0.1)
        for _ in range(3):
            session.add_snapshot(make_snapshot("pipe-a", 0.5))
        results = session.run()
        assert results["pipe-a"].tripped

    def test_tripped_pipelines_returns_only_open_circuits(self):
        session = CircuitBreakerSession(threshold=2, error_threshold=0.1)
        for _ in range(3):
            session.add_snapshot(make_snapshot("pipe-bad", 0.9))
        session.add_snapshot(make_snapshot("pipe-ok", 0.0))
        tripped = session.tripped_pipelines()
        assert "pipe-bad" in tripped
        assert "pipe-ok" not in tripped

    def test_latest_snapshot_used_for_evaluation(self):
        self.session.add_snapshot(make_snapshot("pipe-a", 0.9))
        self.session.add_snapshot(make_snapshot("pipe-a", 0.0))
        results = self.session.run()
        assert not results["pipe-a"].tripped
