from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytest

from pipewatch.pipeline_balancer import BalanceEntry, BalancerResult, balance_snapshots
from pipewatch.balancer_reporter import render_balancer_table, overloaded_summary
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0),
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
        metric_count=1,
    )


class TestRenderBalancerTable:
    def test_none_result_returns_no_pipelines_message(self):
        output = render_balancer_table(None)
        assert "No pipelines" in output

    def test_empty_result_returns_no_pipelines_message(self):
        result = BalancerResult(entries=[])
        output = render_balancer_table(result)
        assert "No pipelines" in output

    def test_output_contains_pipeline_id(self):
        snaps = [make_snapshot("pipe-alpha")]
        result = balance_snapshots(snaps)
        output = render_balancer_table(result)
        assert "pipe-alpha" in output

    def test_output_contains_slot_info(self):
        snaps = [make_snapshot("pipe-beta")]
        result = balance_snapshots(snaps)
        output = render_balancer_table(result)
        assert "slot=" in output

    def test_output_contains_load_info(self):
        snaps = [make_snapshot("pipe-gamma")]
        result = balance_snapshots(snaps)
        output = render_balancer_table(result)
        assert "load=" in output

    def test_overloaded_status_shown(self):
        snaps = [make_snapshot("pipe-heavy", error_rate=0.95, throughput=999.0)]
        result = balance_snapshots(snaps, overload_threshold=0.3)
        output = render_balancer_table(result)
        assert "OVERLOADED" in output

    def test_ok_status_shown_for_healthy(self):
        snaps = [make_snapshot("pipe-light", error_rate=0.0, throughput=0.0)]
        result = balance_snapshots(snaps, overload_threshold=0.9)
        output = render_balancer_table(result)
        assert "OK" in output

    def test_summary_line_present(self):
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = balance_snapshots(snaps)
        output = render_balancer_table(result)
        assert "Balanced:" in output


class TestOverloadedSummary:
    def test_none_returns_no_data_message(self):
        msg = overloaded_summary(None)
        assert "No balance data" in msg

    def test_all_healthy_returns_within_limits_message(self):
        snaps = [make_snapshot("pipe-a", error_rate=0.0)]
        result = balance_snapshots(snaps, overload_threshold=0.9)
        msg = overloaded_summary(result)
        assert "within load limits" in msg

    def test_overloaded_pipelines_listed(self):
        snaps = [make_snapshot("pipe-x", error_rate=0.95, throughput=999.0)]
        result = balance_snapshots(snaps, overload_threshold=0.2)
        msg = overloaded_summary(result)
        assert "pipe-x" in msg
        assert "overloaded" in msg
