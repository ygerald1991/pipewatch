"""Tests for pipewatch.pipeline_grouper."""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.pipeline_grouper import (
    PipelineGroup,
    PipelineGrouper,
    group_by_prefix,
)
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0),
        metric_count=5,
        error_rate=error_rate,
        throughput=throughput,
        status="healthy",
    )


class TestPipelineGroup:
    def test_pipeline_ids(self):
        group = PipelineGroup(name="team_a")
        group.snapshots = [make_snapshot("team_a_pipe1"), make_snapshot("team_a_pipe2")]
        assert group.pipeline_ids() == ["team_a_pipe1", "team_a_pipe2"]

    def test_avg_error_rate(self):
        group = PipelineGroup(name="g")
        group.snapshots = [make_snapshot("p1", error_rate=0.2), make_snapshot("p2", error_rate=0.4)]
        assert group.avg_error_rate() == pytest.approx(0.3)

    def test_avg_error_rate_returns_none_when_empty(self):
        group = PipelineGroup(name="g")
        assert group.avg_error_rate() is None

    def test_avg_throughput(self):
        group = PipelineGroup(name="g")
        group.snapshots = [make_snapshot("p1", throughput=100.0), make_snapshot("p2", throughput=200.0)]
        assert group.avg_throughput() == pytest.approx(150.0)

    def test_size(self):
        group = PipelineGroup(name="g")
        group.snapshots = [make_snapshot("p1"), make_snapshot("p2"), make_snapshot("p3")]
        assert group.size() == 3


class TestPipelineGrouper:
    def test_add_creates_group(self):
        grouper = PipelineGrouper(key_fn=lambda s: "all")
        grouper.add(make_snapshot("pipe1"))
        assert "all" in grouper.group_names()

    def test_snapshots_assigned_to_correct_group(self):
        grouper = PipelineGrouper(key_fn=lambda s: s.pipeline_id[:3])
        grouper.add(make_snapshot("abc_one"))
        grouper.add(make_snapshot("abc_two"))
        grouper.add(make_snapshot("xyz_one"))
        assert grouper.group("abc").size() == 2
        assert grouper.group("xyz").size() == 1

    def test_none_key_skips_snapshot(self):
        grouper = PipelineGrouper(key_fn=lambda s: None)
        grouper.add(make_snapshot("pipe1"))
        assert grouper.groups() == []

    def test_group_returns_none_for_missing_name(self):
        grouper = PipelineGrouper(key_fn=lambda s: "g")
        assert grouper.group("missing") is None


class TestGroupByPrefix:
    def test_groups_by_first_segment(self):
        snapshots = [
            make_snapshot("team_a_pipe1"),
            make_snapshot("team_a_pipe2"),
            make_snapshot("team_b_pipe1"),
        ]
        grouper = group_by_prefix(snapshots)
        assert set(grouper.group_names()) == {"team", "team"}
        # All share the same prefix 'team'
        assert grouper.group("team").size() == 3

    def test_groups_by_first_segment_distinct(self):
        snapshots = [
            make_snapshot("alpha_pipe1"),
            make_snapshot("beta_pipe1"),
            make_snapshot("alpha_pipe2"),
        ]
        grouper = group_by_prefix(snapshots)
        assert grouper.group("alpha").size() == 2
        assert grouper.group("beta").size() == 1

    def test_empty_snapshots_returns_empty_grouper(self):
        grouper = group_by_prefix([])
        assert grouper.groups() == []
