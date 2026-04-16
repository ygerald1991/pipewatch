import pytest
from datetime import datetime, timedelta
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_tracer import TraceSpan, TraceResult, trace_pipeline


def make_snapshot(pipeline_id: str, offset_seconds: int = 0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_seconds),
        metric_count=3,
        avg_error_rate=0.01,
        avg_throughput=100.0,
        status="healthy",
    )


class TestTracePipeline:
    def test_returns_none_for_empty_list(self):
        assert trace_pipeline("p1", []) is None

    def test_returns_result_with_correct_pipeline_id(self):
        snaps = [make_snapshot("p1", 0)]
        result = trace_pipeline("p1", snaps)
        assert result is not None
        assert result.pipeline_id == "p1"

    def test_single_snapshot_produces_one_span(self):
        snaps = [make_snapshot("p1", 0)]
        result = trace_pipeline("p1", snaps)
        assert result.stage_count == 1

    def test_multiple_snapshots_produce_correct_span_count(self):
        snaps = [make_snapshot("p1", i * 10) for i in range(4)]
        result = trace_pipeline("p1", snaps)
        assert result.stage_count == 4

    def test_total_duration_is_sum_of_spans(self):
        snaps = [make_snapshot("p1", i * 10) for i in range(3)]
        result = trace_pipeline("p1", snaps)
        assert result.total_duration == pytest.approx(20.0)

    def test_no_incomplete_spans_when_all_ended(self):
        snaps = [make_snapshot("p1", i * 5) for i in range(3)]
        result = trace_pipeline("p1", snaps)
        assert not result.has_incomplete_spans

    def test_span_str_contains_stage_name(self):
        snaps = [make_snapshot("p1", 0), make_snapshot("p1", 30)]
        result = trace_pipeline("p1", snaps)
        assert "stage_1" in str(result.spans[0])

    def test_span_duration_is_none_when_not_ended(self):
        span = TraceSpan(pipeline_id="p1", stage="s1", started_at=datetime.now())
        assert span.duration_seconds is None
        assert "ongoing" in str(span)

    def test_trace_result_has_incomplete_when_open_span(self):
        result = TraceResult(pipeline_id="p1")
        result.spans.append(TraceSpan(pipeline_id="p1", stage="s1", started_at=datetime.now()))
        assert result.has_incomplete_spans
