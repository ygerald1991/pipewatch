import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_scorer import score_pipelines
from pipewatch.pipeline_scorer_reporter import render_scorer_report, low_score_summary


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=10,
    )


class TestRenderScorerReport:
    def test_none_result_returns_no_data_message(self):
        output = render_scorer_report(None)
        assert "No scoring data" in output

    def test_output_contains_pipeline_id(self):
        result = score_pipelines([make_snapshot("pipe-alpha")])
        output = render_scorer_report(result)
        assert "pipe-alpha" in output

    def test_output_contains_score(self):
        result = score_pipelines([make_snapshot("pipe-x", error_rate=0.1)])
        output = render_scorer_report(result)
        assert "score=" in output

    def test_output_contains_grade(self):
        result = score_pipelines([make_snapshot("pipe-x")])
        output = render_scorer_report(result)
        assert "grade=" in output

    def test_report_shows_total_count(self):
        snapshots = [make_snapshot(f"p{i}") for i in range(4)]
        result = score_pipelines(snapshots)
        output = render_scorer_report(result)
        assert "4" in output

    def test_low_score_summary_all_healthy(self):
        result = score_pipelines([make_snapshot("healthy", error_rate=0.0)])
        msg = low_score_summary(result, threshold=50.0)
        assert "above" in msg

    def test_low_score_summary_identifies_bad_pipeline(self):
        result = score_pipelines([make_snapshot("bad", error_rate=0.99, throughput=0.0)])
        msg = low_score_summary(result, threshold=90.0)
        assert "bad" in msg

    def test_low_score_summary_none_returns_no_data(self):
        msg = low_score_summary(None)
        assert "No data" in msg
