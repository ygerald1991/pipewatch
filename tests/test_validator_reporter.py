import pytest
from pipewatch.pipeline_validator import ValidationResult
from pipewatch.validator_reporter import render_validation_table, failed_summary


def make_result(pipeline_id="pipe-1", violations=None):
    return ValidationResult(pipeline_id=pipeline_id, violations=violations or [])


class TestRenderValidationTable:
    def test_empty_returns_no_results_message(self):
        out = render_validation_table([])
        assert "No validation results" in out

    def test_output_contains_pipeline_id(self):
        result = make_result(pipeline_id="etl-main")
        out = render_validation_table([result])
        assert "etl-main" in out

    def test_pass_status_shown(self):
        result = make_result(violations=[])
        out = render_validation_table([result])
        assert "PASS" in out

    def test_fail_status_shown(self):
        result = make_result(violations=["error_rate too high"])
        out = render_validation_table([result])
        assert "FAIL" in out

    def test_violation_message_in_output(self):
        result = make_result(violations=["throughput below min"])
        out = render_validation_table([result])
        assert "throughput below min" in out

    def test_multiple_results(self):
        results = [make_result(f"p{i}") for i in range(3)]
        out = render_validation_table(results)
        for i in range(3):
            assert f"p{i}" in out


class TestFailedSummary:
    def test_all_pass_returns_positive_message(self):
        results = [make_result()]
        out = failed_summary(results)
        assert "passed" in out.lower()

    def test_failed_pipeline_listed(self):
        results = [make_result("bad-pipe", violations=["er too high"])]
        out = failed_summary(results)
        assert "bad-pipe" in out
        assert "1 pipeline" in out

    def test_multiple_failures_counted(self):
        results = [
            make_result("p1", violations=["v1"]),
            make_result("p2", violations=["v2"]),
        ]
        out = failed_summary(results)
        assert "2 pipeline" in out
