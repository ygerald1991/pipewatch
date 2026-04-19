import pytest
from pipewatch.pipeline_circuit_breaker import PipelineCircuitBreaker, BreakerResult
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, error_rate: float, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=throughput,
        status="healthy" if error_rate < 0.1 else "critical",
    )


class TestPipelineCircuitBreaker:
    def setup_method(self):
        self.breaker = PipelineCircuitBreaker(threshold=3, cooldown_seconds=60)

    def test_new_breaker_circuit_is_closed(self):
        assert not self.breaker.is_open("pipe-a")

    def test_single_failure_does_not_trip(self):
        self.breaker.record_failure("pipe-a")
        assert not self.breaker.is_open("pipe-a")

    def test_failures_at_threshold_trip_circuit(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        assert self.breaker.is_open("pipe-a")

    def test_success_resets_circuit(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        assert self.breaker.is_open("pipe-a")
        self.breaker.record_success("pipe-a")
        assert not self.breaker.is_open("pipe-a")

    def test_evaluate_healthy_snapshot_closes_circuit(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        snap = make_snapshot("pipe-a", error_rate=0.01)
        result = self.breaker.evaluate(snap, error_threshold=0.1)
        assert not result.tripped
        assert result.state == "CLOSED"

    def test_evaluate_failing_snapshot_trips_circuit(self):
        snap = make_snapshot("pipe-a", error_rate=0.5)
        for _ in range(3):
            self.breaker.evaluate(snap, error_threshold=0.1)
        result = self.breaker.evaluate(snap, error_threshold=0.1)
        assert result.tripped
        assert result.state == "OPEN"

    def test_evaluate_result_has_correct_pipeline_id(self):
        snap = make_snapshot("pipe-x", error_rate=0.0)
        result = self.breaker.evaluate(snap)
        assert result.pipeline_id == "pipe-x"

    def test_pipeline_ids_returns_known_pipelines(self):
        self.breaker.record_failure("pipe-a")
        self.breaker.record_failure("pipe-b")
        ids = self.breaker.pipeline_ids()
        assert "pipe-a" in ids
        assert "pipe-b" in ids

    def test_str_representation_shows_status(self):
        for _ in range(3):
            self.breaker.record_failure("pipe-a")
        state = self.breaker._get_state("pipe-a")
        assert "OPEN" in str(state)

    def test_breaker_result_str(self):
        result = BreakerResult(pipeline_id="pipe-a", tripped=True, failure_count=3, state="OPEN")
        assert "TRIPPED" in str(result)
        assert "pipe-a" in str(result)
