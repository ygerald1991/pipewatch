"""Tests for pipewatch.dependency_graph."""

from __future__ import annotations

from datetime import datetime
from typing import List

import pytest

from pipewatch.dependency_graph import (
    DependencyGraph,
    analyze_upstream_impact,
)
from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, status: PipelineStatus) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0),
        metrics=[],
        status=status,
        error_rate=0.0,
        throughput=100.0,
    )


class TestDependencyGraph:
    def test_add_dependency_registers_both_nodes(self):
        g = DependencyGraph()
        g.add_dependency("pipeline_b", "pipeline_a")
        assert "pipeline_b" in g.pipeline_ids()
        assert "pipeline_a" in g.pipeline_ids()

    def test_upstreams_returns_correct_dependencies(self):
        g = DependencyGraph()
        g.add_dependency("pipeline_b", "pipeline_a")
        assert g.upstreams("pipeline_b") == ["pipeline_a"]

    def test_upstreams_returns_empty_for_root_pipeline(self):
        g = DependencyGraph()
        g.add_dependency("pipeline_b", "pipeline_a")
        assert g.upstreams("pipeline_a") == []

    def test_multiple_upstreams(self):
        g = DependencyGraph()
        g.add_dependency("pipeline_c", "pipeline_a")
        g.add_dependency("pipeline_c", "pipeline_b")
        assert set(g.upstreams("pipeline_c")) == {"pipeline_a", "pipeline_b"}

    def test_has_pipeline_returns_false_for_unknown(self):
        g = DependencyGraph()
        assert not g.has_pipeline("unknown")

    def test_has_pipeline_returns_true_after_registration(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        assert g.has_pipeline("b")


class TestAnalyzeUpstreamImpact:
    def test_returns_none_for_unregistered_pipeline(self):
        g = DependencyGraph()
        result = analyze_upstream_impact("ghost", g, {})
        assert result is None

    def test_no_degraded_upstreams_when_all_healthy(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        snapshots = {"a": make_snapshot("a", PipelineStatus.HEALTHY)}
        result = analyze_upstream_impact("b", g, snapshots)
        assert result is not None
        assert not result.has_degraded_upstream
        assert result.degraded_upstreams == []

    def test_detects_warning_upstream(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        snapshots = {"a": make_snapshot("a", PipelineStatus.WARNING)}
        result = analyze_upstream_impact("b", g, snapshots)
        assert result is not None
        assert result.has_degraded_upstream
        assert "a" in result.degraded_upstreams

    def test_detects_critical_upstream(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        snapshots = {"a": make_snapshot("a", PipelineStatus.CRITICAL)}
        result = analyze_upstream_impact("b", g, snapshots)
        assert result.has_degraded_upstream

    def test_missing_snapshot_not_counted_as_degraded(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        result = analyze_upstream_impact("b", g, {})
        assert result is not None
        assert not result.has_degraded_upstream

    def test_str_no_degraded(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        snapshots = {"a": make_snapshot("a", PipelineStatus.HEALTHY)}
        result = analyze_upstream_impact("b", g, snapshots)
        assert "no degraded" in str(result)

    def test_str_with_degraded(self):
        g = DependencyGraph()
        g.add_dependency("b", "a")
        snapshots = {"a": make_snapshot("a", PipelineStatus.CRITICAL)}
        result = analyze_upstream_impact("b", g, snapshots)
        assert "a" in str(result)
