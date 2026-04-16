"""Tests for pipewatch.pipeline_merger."""

import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_merger import MergeResult, merge_snapshots


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=10,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestMergeSnapshots:
    def test_empty_list_returns_empty(self):
        assert merge_snapshots([]) == []

    def test_single_snapshot_produces_one_result(self):
        snap = make_snapshot("pipe-a", error_rate=0.1, throughput=50.0)
        results = merge_snapshots([snap])
        assert len(results) == 1
        assert results[0].pipeline_id == "pipe-a"

    def test_snapshot_count_is_correct(self):
        snaps = [make_snapshot("pipe-a") for _ in range(3)]
        results = merge_snapshots(snaps)
        assert results[0].snapshot_count == 3

    def test_avg_error_rate_calculated_correctly(self):
        snaps = [
            make_snapshot("pipe-a", error_rate=0.1),
            make_snapshot("pipe-a", error_rate=0.3),
        ]
        results = merge_snapshots(snaps)
        assert results[0].avg_error_rate == pytest.approx(0.2)

    def test_avg_throughput_calculated_correctly(self):
        snaps = [
            make_snapshot("pipe-a", throughput=100.0),
            make_snapshot("pipe-a", throughput=200.0),
        ]
        results = merge_snapshots(snaps)
        assert results[0].avg_throughput == pytest.approx(150.0)

    def test_max_error_rate_is_highest(self):
        snaps = [
            make_snapshot("pipe-a", error_rate=0.05),
            make_snapshot("pipe-a", error_rate=0.9),
            make_snapshot("pipe-a", error_rate=0.3),
        ]
        results = merge_snapshots(snaps)
        assert results[0].max_error_rate == pytest.approx(0.9)

    def test_min_throughput_is_lowest(self):
        snaps = [
            make_snapshot("pipe-a", throughput=500.0),
            make_snapshot("pipe-a", throughput=10.0),
            make_snapshot("pipe-a", throughput=300.0),
        ]
        results = merge_snapshots(snaps)
        assert results[0].min_throughput == pytest.approx(10.0)

    def test_multiple_pipelines_grouped_separately(self):
        snaps = [
            make_snapshot("pipe-a"),
            make_snapshot("pipe-b"),
            make_snapshot("pipe-a"),
        ]
        results = merge_snapshots(snaps)
        assert len(results) == 2
        ids = [r.pipeline_id for r in results]
        assert "pipe-a" in ids
        assert "pipe-b" in ids

    def test_results_sorted_by_pipeline_id(self):
        snaps = [
            make_snapshot("pipe-c"),
            make_snapshot("pipe-a"),
            make_snapshot("pipe-b"),
        ]
        results = merge_snapshots(snaps)
        assert [r.pipeline_id for r in results] == ["pipe-a", "pipe-b", "pipe-c"]

    def test_none_error_rate_excluded_from_avg(self):
        snap1 = make_snapshot("pipe-a", error_rate=0.4)
        snap2 = make_snapshot("pipe-a", error_rate=0.0)
        snap2.error_rate = None
        results = merge_snapshots([snap1, snap2])
        assert results[0].avg_error_rate == pytest.approx(0.4)

    def test_str_representation_contains_pipeline_id(self):
        snap = make_snapshot("pipe-x", error_rate=0.05, throughput=80.0)
        result = merge_snapshots([snap])[0]
        assert "pipe-x" in str(result)
