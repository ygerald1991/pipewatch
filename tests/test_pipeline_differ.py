"""Tests for pipeline_differ and differ_reporter."""

from __future__ import annotations

import pytest

from pipewatch.pipeline_differ import diff_snapshots, DiffResult, FieldDiff
from pipewatch.differ_reporter import render_diff_table, changed_fields_summary
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(
    pipeline_id: str = "pipe-1",
    error_rate: float = 0.0,
    throughput: float = 100.0,
    avg_latency: float = 0.5,
    metric_count: int = 10,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=metric_count,
        error_rate=error_rate,
        throughput=throughput,
        avg_latency=avg_latency,
    )


class TestDiffSnapshots:
    def test_identical_snapshots_have_no_changes(self):
        s = make_snapshot()
        result = diff_snapshots(s, s)
        assert not result.has_changes

    def test_changed_error_rate_detected(self):
        before = make_snapshot(error_rate=0.01)
        after = make_snapshot(error_rate=0.05)
        result = diff_snapshots(before, after)
        assert result.has_changes
        assert "error_rate" in result.changed_fields

    def test_delta_is_correct(self):
        before = make_snapshot(throughput=100.0)
        after = make_snapshot(throughput=120.0)
        result = diff_snapshots(before, after)
        tp_diff = next(d for d in result.diffs if d.field_name == "throughput")
        assert tp_diff.delta == pytest.approx(20.0)

    def test_pct_change_is_correct(self):
        before = make_snapshot(error_rate=0.10)
        after = make_snapshot(error_rate=0.15)
        result = diff_snapshots(before, after)
        er_diff = next(d for d in result.diffs if d.field_name == "error_rate")
        assert er_diff.pct_change == pytest.approx(50.0)

    def test_pct_change_none_when_before_is_zero(self):
        before = make_snapshot(error_rate=0.0)
        after = make_snapshot(error_rate=0.05)
        result = diff_snapshots(before, after)
        er_diff = next(d for d in result.diffs if d.field_name == "error_rate")
        assert er_diff.pct_change is None

    def test_pipeline_id_mismatch_raises(self):
        before = make_snapshot(pipeline_id="pipe-a")
        after = make_snapshot(pipeline_id="pipe-b")
        with pytest.raises(ValueError, match="Cannot diff"):
            diff_snapshots(before, after)

    def test_result_contains_all_fields(self):
        before = make_snapshot()
        after = make_snapshot()
        result = diff_snapshots(before, after)
        field_names = {d.field_name for d in result.diffs}
        assert {"error_rate", "throughput", "avg_latency"}.issubset(field_names)

    def test_field_diff_str(self):
        d = FieldDiff("error_rate", 0.01, 0.05, 0.04, 300.0)
        s = str(d)
        assert "error_rate" in s
        assert "300.0%" in s


class TestDifferReporter:
    def test_empty_results_returns_no_data_message(self):
        out = render_diff_table([])
        assert "No diff data" in out

    def test_table_contains_pipeline_id(self):
        before = make_snapshot(pipeline_id="pipe-x", error_rate=0.01)
        after = make_snapshot(pipeline_id="pipe-x", error_rate=0.05)
        result = diff_snapshots(before, after)
        out = render_diff_table([result])
        assert "pipe-x" in out

    def test_table_contains_field_names(self):
        before = make_snapshot()
        after = make_snapshot(throughput=200.0)
        result = diff_snapshots(before, after)
        out = render_diff_table([result])
        assert "throughput" in out
        assert "error_rate" in out

    def test_changed_fields_summary_no_changes(self):
        before = make_snapshot()
        after = make_snapshot()
        result = diff_snapshots(before, after)
        summary = changed_fields_summary([result])
        assert "No field-level changes" in summary

    def test_changed_fields_summary_lists_pipeline(self):
        before = make_snapshot(pipeline_id="pipe-z", error_rate=0.01)
        after = make_snapshot(pipeline_id="pipe-z", error_rate=0.09)
        result = diff_snapshots(before, after)
        summary = changed_fields_summary([result])
        assert "pipe-z" in summary
        assert "error_rate" in summary
