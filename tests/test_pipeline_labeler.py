"""Tests for pipewatch.pipeline_labeler."""

import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_labeler import (
    PipelineLabel,
    label_snapshot,
    label_snapshots,
)


def make_snapshot(
    pipeline_id: str = "pipe-1",
    status: PipelineStatus = PipelineStatus.HEALTHY,
    error_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=status,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=1,
    )


class TestLabelSnapshot:
    def test_healthy_snapshot_has_healthy_label(self):
        snap = make_snapshot(status=PipelineStatus.HEALTHY)
        result = label_snapshot(snap)
        assert "healthy" in result.labels

    def test_warning_snapshot_has_warning_label(self):
        snap = make_snapshot(status=PipelineStatus.WARNING)
        result = label_snapshot(snap)
        assert "warning" in result.labels

    def test_critical_snapshot_has_critical_label(self):
        snap = make_snapshot(status=PipelineStatus.CRITICAL)
        result = label_snapshot(snap)
        assert "critical" in result.labels

    def test_high_error_rate_adds_label(self):
        snap = make_snapshot(error_rate=0.6)
        result = label_snapshot(snap)
        assert "high-error-rate" in result.labels

    def test_elevated_error_rate_adds_label(self):
        snap = make_snapshot(error_rate=0.2)
        result = label_snapshot(snap)
        assert "elevated-error-rate" in result.labels

    def test_low_error_rate_no_extra_label(self):
        snap = make_snapshot(error_rate=0.05)
        result = label_snapshot(snap)
        assert "high-error-rate" not in result.labels
        assert "elevated-error-rate" not in result.labels

    def test_zero_throughput_adds_idle_label(self):
        snap = make_snapshot(throughput=0.0)
        result = label_snapshot(snap)
        assert "idle" in result.labels

    def test_high_throughput_adds_label(self):
        snap = make_snapshot(throughput=1500.0)
        result = label_snapshot(snap)
        assert "high-throughput" in result.labels

    def test_normal_throughput_no_extra_label(self):
        snap = make_snapshot(throughput=200.0)
        result = label_snapshot(snap)
        assert "idle" not in result.labels
        assert "high-throughput" not in result.labels

    def test_pipeline_id_is_preserved(self):
        snap = make_snapshot(pipeline_id="my-pipeline")
        result = label_snapshot(snap)
        assert result.pipeline_id == "my-pipeline"

    def test_str_includes_pipeline_id(self):
        snap = make_snapshot(pipeline_id="pipe-x")
        result = label_snapshot(snap)
        assert "pipe-x" in str(result)


class TestLabelSnapshots:
    def test_returns_empty_dict_for_no_snapshots(self):
        result = label_snapshots([])
        assert result == {}

    def test_keys_match_pipeline_ids(self):
        snaps = [make_snapshot("a"), make_snapshot("b")]
        result = label_snapshots(snaps)
        assert set(result.keys()) == {"a", "b"}

    def test_each_value_is_pipeline_label(self):
        snaps = [make_snapshot("pipe-1")]
        result = label_snapshots(snaps)
        assert isinstance(result["pipe-1"], PipelineLabel)
