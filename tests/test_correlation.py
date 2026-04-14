"""Tests for pipewatch.correlation."""

from __future__ import annotations

from typing import Dict

import pytest

from pipewatch.correlation import CorrelationResult, correlate_pipelines, _pearson
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, metrics: Dict[str, float]) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metrics=metrics,
        metric_count=len(metrics),
        error_rate=metrics.get("error_rate", 0.0),
        throughput=metrics.get("throughput", 0.0),
        status="healthy",
    )


class TestPearson:
    def test_empty_returns_none(self):
        assert _pearson([], []) is None

    def test_single_value_returns_none(self):
        assert _pearson([1.0], [1.0]) is None

    def test_perfect_positive_correlation(self):
        r = _pearson([1, 2, 3, 4], [2, 4, 6, 8])
        assert r == pytest.approx(1.0, abs=1e-6)

    def test_perfect_negative_correlation(self):
        r = _pearson([1, 2, 3, 4], [8, 6, 4, 2])
        assert r == pytest.approx(-1.0, abs=1e-6)

    def test_no_correlation_constant_series_returns_none(self):
        assert _pearson([1, 1, 1], [1, 2, 3]) is None


class TestCorrelatePipelines:
    def _snaps(self, pid, error_rates, throughputs):
        return [
            make_snapshot(pid, {"error_rate": e, "throughput": t})
            for e, t in zip(error_rates, throughputs)
        ]

    def test_empty_snapshots_returns_empty(self):
        assert correlate_pipelines([], [], ["error_rate"]) == []

    def test_returns_result_per_metric(self):
        a = self._snaps("pipe-a", [0.1, 0.2, 0.3], [100, 200, 300])
        b = self._snaps("pipe-b", [0.1, 0.2, 0.3], [100, 200, 300])
        results = correlate_pipelines(a, b, ["error_rate", "throughput"])
        assert len(results) == 2

    def test_significant_flag_set_when_above_threshold(self):
        a = self._snaps("pipe-a", [0.1, 0.2, 0.3, 0.4], [10, 20, 30, 40])
        b = self._snaps("pipe-b", [0.1, 0.2, 0.3, 0.4], [10, 20, 30, 40])
        results = correlate_pipelines(a, b, ["error_rate"], significance_threshold=0.7)
        assert results[0].is_significant is True

    def test_not_significant_when_below_threshold(self):
        a = self._snaps("pipe-a", [0.1, 0.4, 0.2, 0.5], [1, 2, 3, 4])
        b = self._snaps("pipe-b", [0.5, 0.1, 0.4, 0.2], [4, 3, 2, 1])
        results = correlate_pipelines(a, b, ["error_rate"], significance_threshold=0.95)
        assert all(not r.is_significant for r in results)

    def test_pipeline_ids_set_correctly(self):
        a = self._snaps("alpha", [0.1, 0.2, 0.3], [1, 2, 3])
        b = self._snaps("beta", [0.1, 0.2, 0.3], [1, 2, 3])
        results = correlate_pipelines(a, b, ["error_rate"])
        assert results[0].pipeline_a == "alpha"
        assert results[0].pipeline_b == "beta"

    def test_insufficient_data_skips_metric(self):
        a = [make_snapshot("p1", {"error_rate": 0.1})]
        b = [make_snapshot("p2", {"error_rate": 0.2})]
        results = correlate_pipelines(a, b, ["error_rate"])
        assert results == []
