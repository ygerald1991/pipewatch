import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_router import RouteRule, route_snapshots


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, tags=None) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        tags=tags or [],
    )


class TestRouteSnapshots:
    def test_empty_snapshots_returns_empty(self):
        result = route_snapshots([], [])
        assert result.routes == []

    def test_no_rules_uses_default_destination(self):
        snap = make_snapshot("pipe-1")
        result = route_snapshots([snap], [])
        assert len(result.routes) == 1
        assert result.routes[0].destination == "default"
        assert result.routes[0].matched_rule is None

    def test_matching_rule_sets_destination(self):
        snap = make_snapshot("pipe-1", error_rate=0.5)
        rule = RouteRule(min_error_rate=0.3, destination="critical-queue")
        result = route_snapshots([snap], [rule])
        assert result.routes[0].destination == "critical-queue"
        assert result.routes[0].matched_rule is rule

    def test_non_matching_rule_falls_to_default(self):
        snap = make_snapshot("pipe-1", error_rate=0.1)
        rule = RouteRule(min_error_rate=0.3, destination="critical-queue")
        result = route_snapshots([snap], [rule])
        assert result.routes[0].destination == "default"

    def test_first_matching_rule_wins(self):
        snap = make_snapshot("pipe-1", error_rate=0.5)
        rule1 = RouteRule(min_error_rate=0.3, destination="queue-a")
        rule2 = RouteRule(min_error_rate=0.4, destination="queue-b")
        result = route_snapshots([snap], [rule1, rule2])
        assert result.routes[0].destination == "queue-a"

    def test_tag_rule_matches_correctly(self):
        snap = make_snapshot("pipe-1", tags=["critical", "prod"])
        rule = RouteRule(tag="critical", destination="prio-queue")
        result = route_snapshots([snap], [rule])
        assert result.routes[0].destination == "prio-queue"

    def test_tag_rule_does_not_match_missing_tag(self):
        snap = make_snapshot("pipe-1", tags=["dev"])
        rule = RouteRule(tag="critical", destination="prio-queue")
        result = route_snapshots([snap], [rule])
        assert result.routes[0].destination == "default"

    def test_for_destination_filters_correctly(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.5),
            make_snapshot("pipe-2", error_rate=0.01),
        ]
        rule = RouteRule(min_error_rate=0.3, destination="critical-queue")
        result = route_snapshots(snaps, [rule])
        critical = result.for_destination("critical-queue")
        assert len(critical) == 1
        assert critical[0].pipeline_id == "pipe-1"

    def test_destinations_returns_unique_set(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.5),
            make_snapshot("pipe-2", error_rate=0.01),
        ]
        rule = RouteRule(min_error_rate=0.3, destination="critical-queue")
        result = route_snapshots(snaps, [rule])
        dests = result.destinations()
        assert "critical-queue" in dests
        assert "default" in dests

    def test_custom_default_destination(self):
        snap = make_snapshot("pipe-1")
        result = route_snapshots([snap], [], default_destination="fallback")
        assert result.routes[0].destination == "fallback"

    def test_str_representation(self):
        snap = make_snapshot("pipe-1")
        result = route_snapshots([snap], [])
        assert "pipe-1" in str(result.routes[0])
        assert "default" in str(result.routes[0])
