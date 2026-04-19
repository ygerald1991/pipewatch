import pytest
from pipewatch.pipeline_whitelister import PipelineWhitelister, WhitelisterResult
from pipewatch.whitelister_reporter import render_whitelister_table, whitelisted_summary
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        avg_error_rate=0.0,
        avg_throughput=100.0,
        status=PipelineStatus.HEALTHY,
    )


def make_result(whitelisted=None, others=None, default_allow=False):
    w = PipelineWhitelister()
    for pid in (whitelisted or []):
        w.whitelist(pid)
    snapshots = [make_snapshot(pid) for pid in (whitelisted or [])] + \
                [make_snapshot(pid) for pid in (others or [])]
    return w.run(snapshots, default_allow=default_allow)


class TestRenderWhitelisterTable:
    def test_empty_returns_no_pipelines_message(self):
        result = make_result()
        output = render_whitelister_table(result)
        assert "No pipelines" in output

    def test_output_contains_pipeline_id(self):
        result = make_result(whitelisted=["pipe-a"])
        output = render_whitelister_table(result)
        assert "pipe-a" in output

    def test_allowed_status_shown(self):
        result = make_result(whitelisted=["pipe-a"])
        output = render_whitelister_table(result)
        assert "ALLOWED" in output

    def test_blocked_status_shown(self):
        result = make_result(others=["pipe-b"])
        output = render_whitelister_table(result)
        assert "BLOCKED" in output

    def test_summary_counts_shown(self):
        result = make_result(whitelisted=["pipe-a"], others=["pipe-b"])
        output = render_whitelister_table(result)
        assert "Allowed: 1" in output
        assert "Blocked: 1" in output

    def test_whitelisted_summary_lists_ids(self):
        result = make_result(whitelisted=["pipe-a", "pipe-b"])
        summary = whitelisted_summary(result)
        assert "pipe-a" in summary
        assert "pipe-b" in summary

    def test_whitelisted_summary_empty(self):
        result = make_result(others=["pipe-x"])
        summary = whitelisted_summary(result)
        assert "No pipelines" in summary
