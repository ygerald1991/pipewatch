from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.pipeline_diverter import (
    DivertRule,
    DiverterResult,
    DivertEntry,
    divert_snapshots,
)
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    snap = MagicMock(spec=PipelineSnapshot)
    snap.pipeline_id = pipeline_id
    snap.error_rate = error_rate
    return snap


class TestDivertSnapshots:
    def test_empty_returns_empty(self):
        result = divert_snapshots([], rules=[])
        assert isinstance(result, DiverterResult)
        assert result.entries == []

    def test_no_rules_uses_default_destination(self):
        snap = make_snapshot("pipe-1")
        result = divert_snapshots([snap], rules=[], default_destination="primary")
        assert len(result.entries) == 1
        assert result.entries[0].diverted_to == "primary"

    def test_matching_rule_diverts_pipeline(self):
        snap = make_snapshot("pipe-1", error_rate=0.5)
        rule = DivertRule(
            condition=lambda s: s.error_rate > 0.1,
            destination="quarantine",
            reason="high error rate",
        )
        result = divert_snapshots([snap], rules=[rule], default_destination="primary")
        assert result.entries[0].diverted_to == "quarantine"
        assert result.entries[0].reason == "high error rate"

    def test_non_matching_rule_leaves_default(self):
        snap = make_snapshot("pipe-1", error_rate=0.01)
        rule = DivertRule(
            condition=lambda s: s.error_rate > 0.5,
            destination="quarantine",
            reason="high error rate",
        )
        result = divert_snapshots([snap], rules=[rule], default_destination="primary")
        assert result.entries[0].diverted_to == "primary"

    def test_first_matching_rule_wins(self):
        snap = make_snapshot("pipe-1", error_rate=0.3)
        rule1 = DivertRule(
            condition=lambda s: s.error_rate > 0.1,
            destination="slow-lane",
            reason="moderate error",
        )
        rule2 = DivertRule(
            condition=lambda s: s.error_rate > 0.2,
            destination="quarantine",
            reason="high error",
        )
        result = divert_snapshots([snap], rules=[rule1, rule2])
        assert result.entries[0].diverted_to == "slow-lane"

    def test_total_diverted_count(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.5),
            make_snapshot("pipe-2", error_rate=0.01),
            make_snapshot("pipe-3", error_rate=0.8),
        ]
        rule = DivertRule(
            condition=lambda s: s.error_rate > 0.1,
            destination="quarantine",
            reason="high error rate",
        )
        result = divert_snapshots(snaps, rules=[rule], default_destination="primary")
        assert result.total_diverted == 2
        assert result.total_unchanged == 1

    def test_pipeline_ids_returned(self):
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = divert_snapshots(snaps, rules=[])
        assert "pipe-a" in result.pipeline_ids
        assert "pipe-b" in result.pipeline_ids

    def test_for_pipeline_returns_correct_entry(self):
        snap = make_snapshot("pipe-x")
        result = divert_snapshots([snap], rules=[])
        entry = result.for_pipeline("pipe-x")
        assert entry is not None
        assert entry.pipeline_id == "pipe-x"

    def test_for_pipeline_returns_none_when_missing(self):
        result = divert_snapshots([], rules=[])
        assert result.for_pipeline("nonexistent") is None

    def test_divert_entry_str(self):
        entry = DivertEntry(
            pipeline_id="pipe-1",
            original_destination="primary",
            diverted_to="quarantine",
            reason="high error rate",
        )
        s = str(entry)
        assert "pipe-1" in s
        assert "quarantine" in s
