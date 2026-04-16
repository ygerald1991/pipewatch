import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_validator import (
    ValidationRule,
    ValidationResult,
    validate_snapshot,
    validate_snapshots,
)


def make_snapshot(pipeline_id="pipe-1", error_rate=0.01, throughput=100.0):
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=5,
        status="healthy",
    )


class TestValidateSnapshot:
    def test_passes_when_no_rules(self):
        snap = make_snapshot()
        result = validate_snapshot(snap, [])
        assert result.passed

    def test_passes_when_within_bounds(self):
        rule = ValidationRule("r1", "desc", max_error_rate=0.1, min_throughput=50.0)
        snap = make_snapshot(error_rate=0.05, throughput=80.0)
        result = validate_snapshot(snap, [rule])
        assert result.passed
        assert result.violations == []

    def test_fails_when_error_rate_too_high(self):
        rule = ValidationRule("high-er", "desc", max_error_rate=0.05)
        snap = make_snapshot(error_rate=0.10)
        result = validate_snapshot(snap, [rule])
        assert not result.passed
        assert len(result.violations) == 1
        assert "error_rate" in result.violations[0]

    def test_fails_when_throughput_too_low(self):
        rule = ValidationRule("low-tp", "desc", min_throughput=200.0)
        snap = make_snapshot(throughput=50.0)
        result = validate_snapshot(snap, [rule])
        assert not result.passed
        assert "throughput" in result.violations[0]

    def test_multiple_rules_collect_all_violations(self):
        rules = [
            ValidationRule("r1", "d", max_error_rate=0.01),
            ValidationRule("r2", "d", min_throughput=500.0),
        ]
        snap = make_snapshot(error_rate=0.05, throughput=10.0)
        result = validate_snapshot(snap, rules)
        assert len(result.violations) == 2

    def test_pipeline_id_preserved(self):
        snap = make_snapshot(pipeline_id="my-pipe")
        result = validate_snapshot(snap, [])
        assert result.pipeline_id == "my-pipe"

    def test_str_pass(self):
        snap = make_snapshot()
        result = validate_snapshot(snap, [])
        assert "PASS" in str(result)

    def test_str_fail(self):
        rule = ValidationRule("r", "d", max_error_rate=0.0)
        snap = make_snapshot(error_rate=0.5)
        result = validate_snapshot(snap, [rule])
        assert "FAIL" in str(result)

    def test_validate_snapshots_returns_one_per_snapshot(self):
        snaps = [make_snapshot(f"p{i}") for i in range(4)]
        results = validate_snapshots(snaps, [])
        assert len(results) == 4

    def test_validate_snapshots_empty(self):
        results = validate_snapshots([], [])
        assert results == []

    def test_min_error_rate_violation(self):
        rule = ValidationRule("r", "d", min_error_rate=0.1)
        snap = make_snapshot(error_rate=0.01)
        result = validate_snapshot(snap, [rule])
        assert not result.passed

    def test_max_throughput_violation(self):
        rule = ValidationRule("r", "d", max_throughput=50.0)
        snap = make_snapshot(throughput=100.0)
        result = validate_snapshot(snap, [rule])
        assert not result.passed
