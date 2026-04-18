from __future__ import annotations
import pytest
from pipewatch.pipeline_fencer import FencerResult, PipelineFencer
from pipewatch.fencer_reporter import render_fencer_table, fenced_summary


class TestRenderFencerTable:
    def test_empty_result_returns_no_pipelines_message(self):
        result = FencerResult()
        output = render_fencer_table(result)
        assert "No pipelines" in output

    def test_output_contains_fenced_pipeline_id(self):
        result = FencerResult(fenced=["pipe-1"], allowed=["pipe-2"])
        output = render_fencer_table(result)
        assert "pipe-1" in output

    def test_output_contains_allowed_pipeline_id(self):
        result = FencerResult(fenced=["pipe-1"], allowed=["pipe-2"])
        output = render_fencer_table(result)
        assert "pipe-2" in output

    def test_fenced_status_shown(self):
        result = FencerResult(fenced=["pipe-1"], allowed=[])
        output = render_fencer_table(result)
        assert "FENCED" in output

    def test_allowed_status_shown(self):
        result = FencerResult(fenced=[], allowed=["pipe-2"])
        output = render_fencer_table(result)
        assert "allowed" in output

    def test_summary_counts_shown(self):
        result = FencerResult(fenced=["pipe-1"], allowed=["pipe-2", "pipe-3"])
        output = render_fencer_table(result)
        assert "1" in output
        assert "2" in output


class TestFencedSummary:
    def test_no_fenced_pipelines_message(self):
        fencer = PipelineFencer()
        output = fenced_summary(fencer)
        assert "No pipelines" in output

    def test_fenced_pipeline_id_in_summary(self):
        fencer = PipelineFencer()
        fencer.fence("pipe-x", reason="testing")
        output = fenced_summary(fencer)
        assert "pipe-x" in output

    def test_count_shown_in_summary(self):
        fencer = PipelineFencer()
        fencer.fence("pipe-a", reason="r1")
        fencer.fence("pipe-b", reason="r2")
        output = fenced_summary(fencer)
        assert "2" in output
