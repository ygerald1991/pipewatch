"""Tests for pipewatch.pipeline_profiler."""

from __future__ import annotations

from typing import List, Optional

import pytest

from pipewatch.pipeline_profiler import (
    PipelineProfile,
    profile_all,
    profile_pipeline,
)
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(
    pipeline_id: str,
    error_rate: Optional[float] = 0.0,
    throughput: Optional[float] = 100.0,
    metric_count: int = 10,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=metric_count,
        error_rate=error_rate,
        throughput=throughput,
        status="healthy",
    )


class TestProfilePipeline:
    def test_returns_none_for_empty_list(self):
        assert profile_pipeline([]) is None

    def test_sample_count_matches_snapshot_count(self):
        snaps = [make_snapshot("p1") for _ in range(5)]
        profile = profile_pipeline(snaps)
        assert profile is not None
        assert profile.sample_count == 5

    def test_pipeline_id_is_set(self):
        snaps = [make_snapshot("pipe-A")]
        profile = profile_pipeline(snaps)
        assert profile.pipeline_id == "pipe-A"

    def test_avg_error_rate_computed_correctly(self):
        snaps = [
            make_snapshot("p1", error_rate=0.1),
            make_snapshot("p1", error_rate=0.3),
        ]
        profile = profile_pipeline(snaps)
        assert profile.avg_error_rate == pytest.approx(0.2)

    def test_avg_throughput_computed_correctly(self):
        snaps = [
            make_snapshot("p1", throughput=100.0),
            make_snapshot("p1", throughput=200.0),
        ]
        profile = profile_pipeline(snaps)
        assert profile.avg_throughput == pytest.approx(150.0)

    def test_peak_throughput_is_max(self):
        snaps = [
            make_snapshot("p1", throughput=50.0),
            make_snapshot("p1", throughput=300.0),
            make_snapshot("p1", throughput=150.0),
        ]
        profile = profile_pipeline(snaps)
        assert profile.peak_throughput == pytest.approx(300.0)

    def test_min_throughput_is_min(self):
        snaps = [
            make_snapshot("p1", throughput=50.0),
            make_snapshot("p1", throughput=300.0),
        ]
        profile = profile_pipeline(snaps)
        assert profile.min_throughput == pytest.approx(50.0)

    def test_variance_is_none_for_single_sample(self):
        snaps = [make_snapshot("p1", error_rate=0.1)]
        profile = profile_pipeline(snaps)
        assert profile.error_rate_variance is None

    def test_variance_nonzero_for_varying_error_rates(self):
        snaps = [
            make_snapshot("p1", error_rate=0.0),
            make_snapshot("p1", error_rate=1.0),
        ]
        profile = profile_pipeline(snaps)
        assert profile.error_rate_variance is not None
        assert profile.error_rate_variance > 0

    def test_none_error_rates_excluded_from_avg(self):
        snaps = [
            make_snapshot("p1", error_rate=None),
            make_snapshot("p1", error_rate=0.4),
        ]
        profile = profile_pipeline(snaps)
        assert profile.avg_error_rate == pytest.approx(0.4)


class TestProfileAll:
    def test_empty_map_returns_empty(self):
        assert profile_all({}) == {}

    def test_profiles_keyed_by_pipeline_id(self):
        snap_map = {
            "p1": [make_snapshot("p1")],
            "p2": [make_snapshot("p2")],
        }
        result = profile_all(snap_map)
        assert set(result.keys()) == {"p1", "p2"}

    def test_empty_snapshot_list_excluded(self):
        snap_map = {"p1": [make_snapshot("p1")], "p2": []}
        result = profile_all(snap_map)
        assert "p2" not in result
        assert "p1" in result
