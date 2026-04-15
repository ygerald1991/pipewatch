"""Tests for pipewatch.pipeline_summarizer."""

import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_summarizer import (
    PipelineSummary,
    summarize_pipeline,
    summarize_all,
)


def make_snapshot(
    pipeline_id="pipe-1",
    error_rate=0.01,
    throughput=100.0,
    status=PipelineStatus.HEALTHY,
    metric_count=5,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        status=status,
        metric_count=metric_count,
    )


class TestSummarizePipeline:
    def test_returns_none_for_empty_list(self):
        assert summarize_pipeline([]) is None

    def test_pipeline_id_is_set(self):
        snap = make_snapshot(pipeline_id="alpha")
        result = summarize_pipeline([snap])
        assert result.pipeline_id == "alpha"

    def test_snapshot_count_matches(self):
        snaps = [make_snapshot() for _ in range(4)]
        result = summarize_pipeline(snaps)
        assert result.snapshot_count == 4

    def test_avg_error_rate_calculated(self):
        snaps = [
            make_snapshot(error_rate=0.10),
            make_snapshot(error_rate=0.20),
        ]
        result = summarize_pipeline(snaps)
        assert result.avg_error_rate == pytest.approx(0.15)

    def test_avg_throughput_calculated(self):
        snaps = [
            make_snapshot(throughput=100.0),
            make_snapshot(throughput=200.0),
        ]
        result = summarize_pipeline(snaps)
        assert result.avg_throughput == pytest.approx(150.0)

    def test_avg_error_rate_none_when_all_none(self):
        snaps = [make_snapshot(error_rate=None)]
        result = summarize_pipeline(snaps)
        assert result.avg_error_rate is None

    def test_degraded_count_includes_warning_and_critical(self):
        snaps = [
            make_snapshot(status=PipelineStatus.HEALTHY),
            make_snapshot(status=PipelineStatus.WARNING),
            make_snapshot(status=PipelineStatus.CRITICAL),
        ]
        result = summarize_pipeline(snaps)
        assert result.degraded_count == 2

    def test_dominant_status_is_most_frequent(self):
        snaps = [
            make_snapshot(status=PipelineStatus.HEALTHY),
            make_snapshot(status=PipelineStatus.WARNING),
            make_snapshot(status=PipelineStatus.WARNING),
        ]
        result = summarize_pipeline(snaps)
        assert result.dominant_status == PipelineStatus.WARNING

    def test_str_contains_pipeline_id(self):
        snap = make_snapshot(pipeline_id="my-pipe")
        result = summarize_pipeline([snap])
        assert "my-pipe" in str(result)


class TestSummarizeAll:
    def test_empty_dict_returns_empty_list(self):
        assert summarize_all({}) == []

    def test_returns_one_summary_per_pipeline(self):
        groups = {
            "pipe-a": [make_snapshot(pipeline_id="pipe-a")],
            "pipe-b": [make_snapshot(pipeline_id="pipe-b")],
        }
        results = summarize_all(groups)
        assert len(results) == 2

    def test_results_sorted_by_pipeline_id(self):
        groups = {
            "zzz": [make_snapshot(pipeline_id="zzz")],
            "aaa": [make_snapshot(pipeline_id="aaa")],
        }
        results = summarize_all(groups)
        assert results[0].pipeline_id == "aaa"
        assert results[1].pipeline_id == "zzz"

    def test_pipeline_with_no_snapshots_excluded(self):
        groups = {
            "pipe-a": [make_snapshot(pipeline_id="pipe-a")],
            "pipe-empty": [],
        }
        results = summarize_all(groups)
        ids = [r.pipeline_id for r in results]
        assert "pipe-empty" not in ids
        assert "pipe-a" in ids
