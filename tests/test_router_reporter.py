import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_router import RouteRule, route_snapshots
from pipewatch.router_reporter import render_router_table, destination_summary


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=3,
        error_rate=error_rate,
        throughput=50.0,
        status=PipelineStatus.HEALTHY,
        tags=[],
    )


class TestRenderRouterTable:
    def test_empty_result_returns_no_routes_message(self):
        from pipewatch.pipeline_router import RouterResult
        result = RouterResult(routes=[])
        output = render_router_table(result)
        assert "No routes" in output

    def test_output_contains_pipeline_id(self):
        snap = make_snapshot("alpha")
        result = route_snapshots([snap], [])
        output = render_router_table(result)
        assert "alpha" in output

    def test_output_contains_destination(self):
        snap = make_snapshot("beta", error_rate=0.8)
        rule = RouteRule(min_error_rate=0.5, destination="hot-queue")
        result = route_snapshots([snap], [rule])
        output = render_router_table(result)
        assert "hot-queue" in output

    def test_matched_rule_shows_rule_label(self):
        snap = make_snapshot("gamma", error_rate=0.9)
        rule = RouteRule(min_error_rate=0.5, destination="hot-queue")
        result = route_snapshots([snap], [rule])
        output = render_router_table(result)
        assert "rule" in output

    def test_unmatched_shows_default_label(self):
        snap = make_snapshot("delta")
        result = route_snapshots([snap], [])
        output = render_router_table(result)
        assert "default" in output


class TestDestinationSummary:
    def test_empty_result_returns_no_destinations(self):
        from pipewatch.pipeline_router import RouterResult
        result = RouterResult(routes=[])
        output = destination_summary(result)
        assert "No destinations" in output

    def test_summary_contains_destination_name(self):
        snap = make_snapshot("pipe-1", error_rate=0.7)
        rule = RouteRule(min_error_rate=0.5, destination="critical")
        result = route_snapshots([snap], [rule])
        output = destination_summary(result)
        assert "critical" in output

    def test_summary_counts_are_correct(self):
        snaps = [make_snapshot(f"p{i}") for i in range(3)]
        result = route_snapshots(snaps, [])
        output = destination_summary(result)
        assert "default=3" in output
