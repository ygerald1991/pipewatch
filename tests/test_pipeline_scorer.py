"""Tests for pipeline_scorer and scorer_reporter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.metrics import PipelineMetric
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_scorer import score_pipelines, ScoringResult
from pipewatch.scorer_reporter import render_scoring_table, overall_scoring_status


def make_metric(pipeline_id: str, success: int, failure: int, processed: int) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(tz=timezone.utc),
        success_count=success,
        failure_count=failure,
        records_processed=processed,
        duration_seconds=60.0,
    )


def make_snapshot(pipeline_id: str, metrics: List[PipelineMetric]) -> PipelineSnapshot:
    return PipelineSnapshot(pipeline_id=pipeline_id, metrics=metrics)


class TestScorePipelines:
    def test_empty_snapshots_returns_empty_result(self):
        result = score_pipelines({})
        assert result.scores == []

    def test_single_healthy_pipeline_ranked_first(self):
        m = make_metric("pipe-a", success=100, failure=0, processed=100)
        snap = make_snapshot("pipe-a", [m])
        result = score_pipelines({"pipe-a": snap})
        assert len(result.scores) == 1
        assert result.scores[0].rank == 1
        assert result.scores[0].pipeline_id == "pipe-a"

    def test_healthier_pipeline_ranked_higher(self):
        healthy = make_snapshot("healthy", [make_metric("healthy", 100, 0, 100)])
        sick = make_snapshot("sick", [make_metric("sick", 10, 90, 100)])
        result = score_pipelines({"healthy": healthy, "sick": sick})
        ids = [s.pipeline_id for s in result.scores]
        assert ids.index("healthy") < ids.index("sick")

    def test_ranks_are_sequential(self):
        snaps = {
            f"pipe-{i}": make_snapshot(f"pipe-{i}", [make_metric(f"pipe-{i}", 100 - i * 10, i * 10, 100)])
            for i in range(4)
        }
        result = score_pipelines(snaps)
        ranks = [s.rank for s in result.scores]
        assert ranks == list(range(1, len(ranks) + 1))

    def test_snapshot_with_no_metrics_excluded(self):
        empty_snap = make_snapshot("empty", [])
        result = score_pipelines({"empty": empty_snap})
        assert result.scores == []

    def test_by_id_returns_correct_entry(self):
        snap = make_snapshot("pipe-x", [make_metric("pipe-x", 80, 20, 100)])
        result = score_pipelines({"pipe-x": snap})
        entry = result.by_id("pipe-x")
        assert entry is not None
        assert entry.pipeline_id == "pipe-x"

    def test_by_id_returns_none_for_missing(self):
        result = score_pipelines({})
        assert result.by_id("nope") is None

    def test_top_returns_correct_slice(self):
        snaps = {
            f"p{i}": make_snapshot(f"p{i}", [make_metric(f"p{i}", 100 - i * 5, i * 5, 100)])
            for i in range(6)
        }
        result = score_pipelines(snaps)
        top3 = result.top(3)
        assert len(top3) == 3
        assert all(s.rank <= 3 for s in top3)


class TestScorerReporter:
    def test_empty_result_returns_no_data_message(self):
        result = ScoringResult(scores=[])
        out = render_scoring_table(result)
        assert "No pipeline scores" in out

    def test_table_contains_pipeline_id(self):
        snap = make_snapshot("pipe-a", [make_metric("pipe-a", 100, 0, 100)])
        result = score_pipelines({"pipe-a": snap})
        out = render_scoring_table(result)
        assert "pipe-a" in out

    def test_overall_status_all_healthy(self):
        snap = make_snapshot("pipe-a", [make_metric("pipe-a", 100, 0, 100)])
        result = score_pipelines({"pipe-a": snap})
        status = overall_scoring_status(result)
        assert "healthy" in status

    def test_overall_status_degraded(self):
        snap = make_snapshot("bad", [make_metric("bad", 5, 95, 100)])
        result = score_pipelines({"bad": snap})
        status = overall_scoring_status(result)
        assert "degraded" in status

    def test_overall_status_empty(self):
        result = ScoringResult(scores=[])
        status = overall_scoring_status(result)
        assert "No pipelines" in status
