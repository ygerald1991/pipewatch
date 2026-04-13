"""Tests for pipewatch.trend_reporter."""

import pytest
from pipewatch.trend_analyzer import TrendResult
from pipewatch.trend_reporter import (
    render_trend_table,
    overall_trend_status,
)


def make_trend(pipeline_id="pipe-1", metric="error_rate", direction="stable", delta=0.0, n=5):
    return TrendResult(
        pipeline_id=pipeline_id,
        metric_name=metric,
        direction=direction,
        delta=delta,
        sample_count=n,
    )


class TestRenderTrendTable:
    def test_empty_data_returns_no_data_message(self):
        output = render_trend_table({})
        assert "No trend data" in output

    def test_output_contains_pipeline_id(self):
        data = {"my-pipeline": [make_trend(pipeline_id="my-pipeline")]}
        output = render_trend_table(data)
        assert "my-pipeline" in output

    def test_output_contains_metric_name(self):
        data = {"p1": [make_trend(metric="throughput")]}
        output = render_trend_table(data)
        assert "throughput" in output

    def test_output_contains_direction(self):
        data = {"p1": [make_trend(direction="degrading")]}
        output = render_trend_table(data)
        assert "degrading" in output

    def test_insufficient_data_message_when_no_results(self):
        data = {"p1": []}
        output = render_trend_table(data)
        assert "insufficient" in output

    def test_multiple_pipelines_all_present(self):
        data = {
            "alpha": [make_trend(pipeline_id="alpha")],
            "beta": [make_trend(pipeline_id="beta")],
        }
        output = render_trend_table(data)
        assert "alpha" in output
        assert "beta" in output


class TestOverallTrendStatus:
    def test_ok_when_no_degrading(self):
        data = {"p1": [make_trend(direction="stable"), make_trend(direction="improving")]}
        assert overall_trend_status(data) == "ok"

    def test_warning_when_one_or_two_degrading(self):
        data = {
            "p1": [make_trend(direction="degrading")],
            "p2": [make_trend(direction="stable")],
        }
        assert overall_trend_status(data) == "warning"

    def test_critical_when_many_degrading(self):
        data = {
            "p1": [make_trend(direction="degrading"), make_trend(direction="degrading")],
            "p2": [make_trend(direction="degrading")],
        }
        assert overall_trend_status(data) == "critical"

    def test_ok_when_empty(self):
        assert overall_trend_status({}) == "ok"
