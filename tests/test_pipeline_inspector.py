import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_inspector import inspect_pipeline, InspectionResult
from pipewatch.inspector_session import InspectorSession


def make_snapshot(pipeline_id, error_rate, throughput):
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=1,
    )


class TestInspectPipeline:
    def test_returns_none_for_empty_list(self):
        assert inspect_pipeline("p1", []) is None

    def test_returns_result_with_correct_pipeline_id(self):
        snaps = [make_snapshot("p1", 0.01, 50.0)]
        result = inspect_pipeline("p1", snaps)
        assert result.pipeline_id == "p1"

    def test_snapshot_count_matches(self):
        snaps = [make_snapshot("p1", 0.01, 50.0)] * 3
        result = inspect_pipeline("p1", snaps)
        assert result.snapshot_count == 3

    def test_no_flags_for_healthy_pipeline(self):
        snaps = [make_snapshot("p1", 0.01, 50.0)]
        result = inspect_pipeline("p1", snaps)
        assert result.flags == []

    def test_high_error_rate_flag(self):
        snaps = [make_snapshot("p1", 0.10, 50.0)]
        result = inspect_pipeline("p1", snaps, error_rate_warn=0.05)
        assert "high_error_rate" in result.flags

    def test_low_throughput_flag(self):
        snaps = [make_snapshot("p1", 0.01, 5.0)]
        result = inspect_pipeline("p1", snaps, throughput_warn=10.0)
        assert "low_throughput" in result.flags

    def test_critical_error_spike_flag(self):
        snaps = [make_snapshot("p1", 0.6, 50.0)]
        result = inspect_pipeline("p1", snaps)
        assert "critical_error_spike" in result.flags

    def test_avg_error_rate_calculated(self):
        snaps = [make_snapshot("p1", 0.1, 50.0), make_snapshot("p1", 0.3, 50.0)]
        result = inspect_pipeline("p1", snaps)
        assert abs(result.avg_error_rate - 0.2) < 1e-9

    def test_max_error_rate_calculated(self):
        snaps = [make_snapshot("p1", 0.1, 50.0), make_snapshot("p1", 0.4, 50.0)]
        result = inspect_pipeline("p1", snaps)
        assert result.max_error_rate == pytest.approx(0.4)

    def test_min_throughput_calculated(self):
        snaps = [make_snapshot("p1", 0.01, 20.0), make_snapshot("p1", 0.01, 5.0)]
        result = inspect_pipeline("p1", snaps)
        assert result.min_throughput == pytest.approx(5.0)


class TestInspectorSession:
    def test_empty_session_returns_empty(self):
        session = InspectorSession()
        assert session.run() == []

    def test_pipeline_ids_registered(self):
        session = InspectorSession()
        session.add_snapshot(make_snapshot("p1", 0.01, 50.0))
        session.add_snapshot(make_snapshot("p2", 0.02, 40.0))
        assert set(session.pipeline_ids()) == {"p1", "p2"}

    def test_run_returns_one_result_per_pipeline(self):
        session = InspectorSession()
        session.add_snapshot(make_snapshot("p1", 0.01, 50.0))
        session.add_snapshot(make_snapshot("p2", 0.02, 40.0))
        results = session.run()
        assert len(results) == 2

    def test_run_result_pipeline_ids_match(self):
        session = InspectorSession()
        session.add_snapshot(make_snapshot("p1", 0.01, 50.0))
        session.add_snapshot(make_snapshot("p2", 0.02, 40.0))
        results = session.run()
        result_ids = {r.pipeline_id for r in results}
        assert result_ids == {"p1", "p2"}
