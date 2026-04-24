from __future__ import annotations

from datetime import datetime
from typing import List, Optional

import pytest

from pipewatch.pipeline_balancer import (
    BalancerResult,
    balance_snapshots,
    _load_score,
)
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(
    pipeline_id: str,
    error_rate: Optional[float] = 0.0,
    throughput: Optional[float] = 100.0,
    status: PipelineStatus = PipelineStatus.HEALTHY,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0),
        error_rate=error_rate,
        throughput=throughput,
        status=status,
        metric_count=1,
    )


class TestBalanceSnapshots:
    def test_empty_returns_empty(self):
        result = balance_snapshots([])
        assert isinstance(result, BalancerResult)
        assert result.entries == []

    def test_single_pipeline_assigned_slot_one(self):
        snap = make_snapshot("pipe-a")
        result = balance_snapshots([snap], num_slots=4)
        assert len(result.entries) == 1
        assert result.entries[0].pipeline_id == "pipe-a"
        assert result.entries[0].assigned_slot == 1

    def test_slots_wrap_around(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(6)]
        result = balance_snapshots(snaps, num_slots=4)
        slots = [e.assigned_slot for e in result.entries]
        assert max(slots) <= 4
        assert min(slots) >= 1

    def test_overloaded_flag_set_above_threshold(self):
        snap = make_snapshot("pipe-heavy", error_rate=0.9, throughput=900.0)
        result = balance_snapshots([snap], overload_threshold=0.5)
        assert result.entries[0].overloaded is True

    def test_not_overloaded_below_threshold(self):
        snap = make_snapshot("pipe-light", error_rate=0.0, throughput=0.0)
        result = balance_snapshots([snap], overload_threshold=0.5)
        assert result.entries[0].overloaded is False

    def test_total_overloaded_count(self):
        snaps = [
            make_snapshot("pipe-a", error_rate=0.9, throughput=900.0),
            make_snapshot("pipe-b", error_rate=0.0, throughput=0.0),
        ]
        result = balance_snapshots(snaps, overload_threshold=0.5)
        assert result.total_overloaded == 1
        assert result.total_balanced == 1

    def test_pipeline_ids_present(self):
        snaps = [make_snapshot("alpha"), make_snapshot("beta")]
        result = balance_snapshots(snaps)
        assert "alpha" in result.pipeline_ids
        assert "beta" in result.pipeline_ids

    def test_for_pipeline_returns_entry(self):
        snap = make_snapshot("pipe-x")
        result = balance_snapshots([snap])
        entry = result.for_pipeline("pipe-x")
        assert entry is not None
        assert entry.pipeline_id == "pipe-x"

    def test_for_pipeline_returns_none_for_unknown(self):
        snap = make_snapshot("pipe-x")
        result = balance_snapshots([snap])
        assert result.for_pipeline("unknown") is None

    def test_load_score_zero_for_idle_pipeline(self):
        snap = make_snapshot("idle", error_rate=0.0, throughput=0.0)
        assert _load_score(snap) == 0.0

    def test_load_score_bounded(self):
        snap = make_snapshot("busy", error_rate=1.0, throughput=10_000.0)
        score = _load_score(snap)
        assert 0.0 <= score <= 1.0
