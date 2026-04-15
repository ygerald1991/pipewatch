"""Tests for pipewatch.pipeline_filter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.pipeline_filter import FilterCriteria, filter_snapshots, partition_snapshots
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    throughput: float = 100.0,
    tags: List[str] | None = None,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        tags=tags or [],
    )


class TestFilterSnapshots:
    def test_empty_criteria_returns_all(self):
        snaps = [make_snapshot("a"), make_snapshot("b")]
        result = filter_snapshots(snaps, FilterCriteria())
        assert len(result) == 2

    def test_filter_by_pipeline_id(self):
        snaps = [make_snapshot("a"), make_snapshot("b"), make_snapshot("c")]
        result = filter_snapshots(snaps, FilterCriteria(pipeline_ids=["a", "c"]))
        assert [s.pipeline_id for s in result] == ["a", "c"]

    def test_filter_by_min_error_rate(self):
        snaps = [make_snapshot("a", error_rate=0.1), make_snapshot("b", error_rate=0.5)]
        result = filter_snapshots(snaps, FilterCriteria(min_error_rate=0.3))
        assert len(result) == 1
        assert result[0].pipeline_id == "b"

    def test_filter_by_max_error_rate(self):
        snaps = [make_snapshot("a", error_rate=0.1), make_snapshot("b", error_rate=0.5)]
        result = filter_snapshots(snaps, FilterCriteria(max_error_rate=0.2))
        assert len(result) == 1
        assert result[0].pipeline_id == "a"

    def test_filter_by_min_throughput(self):
        snaps = [make_snapshot("a", throughput=50.0), make_snapshot("b", throughput=200.0)]
        result = filter_snapshots(snaps, FilterCriteria(min_throughput=100.0))
        assert len(result) == 1
        assert result[0].pipeline_id == "b"

    def test_filter_by_max_throughput(self):
        snaps = [make_snapshot("a", throughput=50.0), make_snapshot("b", throughput=200.0)]
        result = filter_snapshots(snaps, FilterCriteria(max_throughput=100.0))
        assert len(result) == 1
        assert result[0].pipeline_id == "a"

    def test_filter_by_required_tags(self):
        snaps = [
            make_snapshot("a", tags=["prod", "critical"]),
            make_snapshot("b", tags=["dev"]),
        ]
        result = filter_snapshots(snaps, FilterCriteria(required_tags=["prod"]))
        assert len(result) == 1
        assert result[0].pipeline_id == "a"

    def test_filter_by_custom_predicate(self):
        snaps = [make_snapshot("a", error_rate=0.0), make_snapshot("b", error_rate=0.9)]
        result = filter_snapshots(
            snaps, FilterCriteria(custom=lambda s: s.error_rate is not None and s.error_rate > 0.5)
        )
        assert len(result) == 1
        assert result[0].pipeline_id == "b"

    def test_combined_criteria(self):
        snaps = [
            make_snapshot("a", error_rate=0.1, throughput=300.0),
            make_snapshot("b", error_rate=0.6, throughput=300.0),
            make_snapshot("c", error_rate=0.1, throughput=50.0),
        ]
        criteria = FilterCriteria(max_error_rate=0.2, min_throughput=100.0)
        result = filter_snapshots(snaps, criteria)
        assert len(result) == 1
        assert result[0].pipeline_id == "a"


class TestPartitionSnapshots:
    def test_partition_splits_correctly(self):
        snaps = [make_snapshot("a", error_rate=0.8), make_snapshot("b", error_rate=0.1)]
        matching, rest = partition_snapshots(snaps, FilterCriteria(min_error_rate=0.5))
        assert len(matching) == 1
        assert matching[0].pipeline_id == "a"
        assert len(rest) == 1
        assert rest[0].pipeline_id == "b"

    def test_all_match(self):
        snaps = [make_snapshot("a"), make_snapshot("b")]
        matching, rest = partition_snapshots(snaps, FilterCriteria())
        assert len(matching) == 2
        assert len(rest) == 0

    def test_none_match(self):
        snaps = [make_snapshot("a", error_rate=0.1)]
        matching, rest = partition_snapshots(snaps, FilterCriteria(min_error_rate=0.9))
        assert len(matching) == 0
        assert len(rest) == 1
