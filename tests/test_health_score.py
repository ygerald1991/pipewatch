"""Tests for health_score and health_reporter modules."""

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.health_score import HealthScore, compute_health_score, _grade
from pipewatch.health_reporter import render_health_table, overall_health_status


def make_metric(pipeline_id="pipe-1", success=90, failure=10, duration=60.0):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        success_count=success,
        failure_count=failure,
        total_duration_seconds=duration,
    )


class TestGrade:
    def test_perfect_score_is_A(self):
        assert _grade(100.0) == "A"

    def test_score_90_is_A(self):
        assert _grade(90.0) == "A"

    def test_score_75_is_B(self):
        assert _grade(75.0) == "B"

    def test_score_60_is_C(self):
        assert _grade(60.0) == "C"

    def test_score_40_is_D(self):
        assert _grade(40.0) == "D"

    def test_score_below_40_is_F(self):
        assert _grade(39.9) == "F"
        assert _grade(0.0) == "F"


class TestComputeHealthScore:
    def test_no_failures_gives_perfect_error_component(self):
        metric = make_metric(success=100, failure=0)
        hs = compute_health_score(metric)
        assert hs.score == 100.0
        assert hs.grade == "A"

    def test_all_failures_gives_zero_score(self):
        metric = make_metric(success=0, failure=100)
        hs = compute_health_score(metric)
        assert hs.score == 0.0
        assert hs.grade == "F"

    def test_10_percent_error_rate_score(self):
        metric = make_metric(success=90, failure=10, duration=100.0)
        hs = compute_health_score(metric)
        assert abs(hs.score - 90.0) < 0.1

    def test_pipeline_id_preserved(self):
        metric = make_metric(pipeline_id="etl-sales")
        hs = compute_health_score(metric)
        assert hs.pipeline_id == "etl-sales"

    def test_with_max_throughput_blends_components(self):
        # 0% error (100 pts), throughput = 1.5/s, max = 3.0/s => 50 pts
        metric = make_metric(success=90, failure=0, duration=60.0)
        hs = compute_health_score(metric, max_throughput=3.0)
        # error_weight=0.7 * 100 + throughput_weight=0.3 * 50 = 70 + 15 = 85
        assert abs(hs.score - 85.0) < 0.5

    def test_score_clamped_to_100(self):
        metric = make_metric(success=1000, failure=0, duration=1.0)
        hs = compute_health_score(metric, max_throughput=1.0)
        assert hs.score <= 100.0

    def test_zero_duration_does_not_raise(self):
        metric = make_metric(success=10, failure=0, duration=0.0)
        hs = compute_health_score(metric)
        assert hs.score >= 0.0


class TestHealthReporter:
    def test_empty_returns_no_data_message(self):
        result = render_health_table([])
        assert "No health data" in result

    def test_table_contains_pipeline_id(self):
        metric = make_metric(pipeline_id="pipe-alpha")
        hs = compute_health_score(metric)
        result = render_health_table([hs])
        assert "pipe-alpha" in result

    def test_table_contains_grade(self):
        metric = make_metric(success=100, failure=0)
        hs = compute_health_score(metric)
        result = render_health_table([hs])
        assert "A" in result

    def test_overall_healthy_when_all_high(self):
        scores = [compute_health_score(make_metric(success=100, failure=0, pipeline_id=f"p{i}")) for i in range(3)]
        status = overall_health_status(scores)
        assert "HEALTHY" in status

    def test_overall_critical_when_score_low(self):
        scores = [compute_health_score(make_metric(success=0, failure=100, pipeline_id="bad-pipe"))]
        status = overall_health_status(scores)
        assert "CRITICAL" in status

    def test_overall_unknown_when_empty(self):
        status = overall_health_status([])
        assert "UNKNOWN" in status
