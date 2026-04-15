"""Tests for pipeline_ranker and ranker_reporter."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_ranker import RankEntry, RankingResult, rank_pipelines
from pipewatch.ranker_reporter import render_ranking_table, overall_ranking_status


def make_snapshot(
    pipeline_id: str,
    error_rate: float = 0.0,
    avg_throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        metric_count=5,
        error_rate=error_rate,
        avg_throughput=avg_throughput,
        status="healthy",
    )


class TestRankPipelines:
    def test_empty_returns_empty_result(self):
        result = rank_pipelines([])
        assert result.entries == []

    def test_single_pipeline_ranked_first(self):
        snap = make_snapshot("pipe-a")
        result = rank_pipelines([snap])
        assert len(result.entries) == 1
        assert result.entries[0].rank == 1
        assert result.entries[0].pipeline_id == "pipe-a"

    def test_lower_error_rate_ranks_higher(self):
        snaps = [
            make_snapshot("pipe-bad", error_rate=0.5),
            make_snapshot("pipe-good", error_rate=0.01),
        ]
        result = rank_pipelines(snaps)
        assert result.entries[0].pipeline_id == "pipe-good"
        assert result.entries[1].pipeline_id == "pipe-bad"

    def test_ranks_are_sequential(self):
        snaps = [make_snapshot(f"pipe-{i}", error_rate=i * 0.1) for i in range(5)]
        result = rank_pipelines(snaps)
        ranks = [e.rank for e in result.entries]
        assert ranks == list(range(1, 6))

    def test_composite_score_decreases_with_rank(self):
        snaps = [
            make_snapshot("pipe-a", error_rate=0.0),
            make_snapshot("pipe-b", error_rate=0.3),
            make_snapshot("pipe-c", error_rate=0.7),
        ]
        result = rank_pipelines(snaps)
        scores = [e.composite_score for e in result.entries]
        assert scores == sorted(scores, reverse=True)

    def test_top_returns_n_entries(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(10)]
        result = rank_pipelines(snaps)
        assert len(result.top(3)) == 3

    def test_bottom_returns_n_entries(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(10)]
        result = rank_pipelines(snaps)
        assert len(result.bottom(3)) == 3

    def test_for_pipeline_returns_correct_entry(self):
        snaps = [make_snapshot("alpha"), make_snapshot("beta")]
        result = rank_pipelines(snaps)
        entry = result.for_pipeline("alpha")
        assert entry is not None
        assert entry.pipeline_id == "alpha"

    def test_for_pipeline_returns_none_for_unknown(self):
        result = rank_pipelines([make_snapshot("alpha")])
        assert result.for_pipeline("unknown") is None


class TestRankerReporter:
    def test_empty_result_returns_no_data_message(self):
        output = render_ranking_table(RankingResult())
        assert "No ranking data" in output

    def test_output_contains_pipeline_id(self):
        result = rank_pipelines([make_snapshot("my-pipeline")])
        output = render_ranking_table(result)
        assert "my-pipeline" in output

    def test_limit_restricts_rows(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(10)]
        result = rank_pipelines(snaps)
        output = render_ranking_table(result, limit=3)
        # Count data rows (excluding header and separator)
        data_lines = [l for l in output.splitlines() if l.startswith("#")]
        assert len(data_lines) == 3

    def test_overall_status_contains_worst_pipeline(self):
        snaps = [
            make_snapshot("best-pipe", error_rate=0.0),
            make_snapshot("worst-pipe", error_rate=0.9),
        ]
        result = rank_pipelines(snaps)
        status = overall_ranking_status(result)
        assert "worst-pipe" in status

    def test_overall_status_empty(self):
        status = overall_ranking_status(RankingResult())
        assert "No pipelines ranked" in status
