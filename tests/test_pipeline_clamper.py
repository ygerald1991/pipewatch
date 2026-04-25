import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_clamper import clamp_snapshots, ClamperResult
from pipewatch.clamper_reporter import render_clamper_table, clamped_summary


def make_snapshot(pipeline_id: str, error_rate=None, throughput=None) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=1,
        status=None,
    )


class TestClampSnapshots:
    def test_empty_returns_empty(self):
        result = clamp_snapshots([])
        assert isinstance(result, ClamperResult)
        assert result.entries == []
        assert result.total_clamped == 0

    def test_single_snapshot_within_bounds_not_clamped(self):
        snap = make_snapshot("pipe-a", error_rate=0.05, throughput=100.0)
        result = clamp_snapshots([snap])
        entry = result.for_pipeline("pipe-a")
        assert entry is not None
        assert entry.clamped_error_rate == 0.05
        assert entry.clamped_throughput == 100.0
        assert result.total_clamped == 0

    def test_error_rate_clamped_to_max(self):
        snap = make_snapshot("pipe-b", error_rate=1.5, throughput=50.0)
        result = clamp_snapshots([snap], max_error_rate=1.0)
        entry = result.for_pipeline("pipe-b")
        assert entry.clamped_error_rate == 1.0
        assert result.total_clamped == 1

    def test_error_rate_clamped_to_min(self):
        snap = make_snapshot("pipe-c", error_rate=-0.1, throughput=10.0)
        result = clamp_snapshots([snap], min_error_rate=0.0)
        entry = result.for_pipeline("pipe-c")
        assert entry.clamped_error_rate == 0.0
        assert result.total_clamped == 1

    def test_throughput_clamped_to_max(self):
        snap = make_snapshot("pipe-d", error_rate=0.01, throughput=9999.0)
        result = clamp_snapshots([snap], max_throughput=1000.0)
        entry = result.for_pipeline("pipe-d")
        assert entry.clamped_throughput == 1000.0
        assert result.total_clamped == 1

    def test_none_values_pass_through(self):
        snap = make_snapshot("pipe-e", error_rate=None, throughput=None)
        result = clamp_snapshots([snap])
        entry = result.for_pipeline("pipe-e")
        assert entry.clamped_error_rate is None
        assert entry.clamped_throughput is None
        assert result.total_clamped == 0

    def test_pipeline_ids_correct(self):
        snaps = [
            make_snapshot("p1", error_rate=0.1),
            make_snapshot("p2", error_rate=0.2),
        ]
        result = clamp_snapshots(snaps)
        assert set(result.pipeline_ids) == {"p1", "p2"}

    def test_for_pipeline_returns_none_for_unknown(self):
        result = clamp_snapshots([])
        assert result.for_pipeline("ghost") is None


class TestClamperReporter:
    def test_empty_result_returns_no_pipelines_message(self):
        result = clamp_snapshots([])
        output = render_clamper_table(result)
        assert "No pipelines" in output

    def test_none_result_returns_no_pipelines_message(self):
        output = render_clamper_table(None)
        assert "No pipelines" in output

    def test_output_contains_pipeline_id(self):
        snap = make_snapshot("my-pipeline", error_rate=0.3, throughput=200.0)
        result = clamp_snapshots([snap])
        output = render_clamper_table(result)
        assert "my-pipeline" in output

    def test_clamped_entry_marked_with_asterisk(self):
        snap = make_snapshot("pipe-x", error_rate=2.0, throughput=50.0)
        result = clamp_snapshots([snap], max_error_rate=1.0)
        output = render_clamper_table(result)
        assert "*" in output

    def test_summary_shows_clamped_count(self):
        snap = make_snapshot("pipe-y", error_rate=5.0, throughput=10.0)
        result = clamp_snapshots([snap], max_error_rate=1.0)
        summary = clamped_summary(result)
        assert "1/1" in summary

    def test_summary_none_returns_no_pipelines(self):
        summary = clamped_summary(None)
        assert "No pipelines" in summary
