import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_dispatcher import DispatchRule, dispatch_snapshots


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
        metric_count=5,
    )


class TestDispatchSnapshots:
    def test_empty_returns_empty(self):
        result = dispatch_snapshots([], [])
        assert result.results == []

    def test_no_rules_uses_default(self):
        s = make_snapshot("pipe-a")
        result = dispatch_snapshots([s], [], default_target="fallback")
        assert result.results[0].target == "fallback"
        assert result.results[0].rule_name is None

    def test_no_rules_no_default_gives_none_target(self):
        s = make_snapshot("pipe-a")
        result = dispatch_snapshots([s], [])
        assert result.results[0].target is None

    def test_matching_rule_sets_target(self):
        rule = DispatchRule("high-error", lambda s: s.error_rate > 0.1, "critical-queue")
        s = make_snapshot("pipe-a", error_rate=0.5)
        result = dispatch_snapshots([s], [rule])
        assert result.results[0].target == "critical-queue"
        assert result.results[0].rule_name == "high-error"

    def test_non_matching_rule_falls_to_default(self):
        rule = DispatchRule("high-error", lambda s: s.error_rate > 0.5, "critical-queue")
        s = make_snapshot("pipe-a", error_rate=0.1)
        result = dispatch_snapshots([s], [rule], default_target="normal-queue")
        assert result.results[0].target == "normal-queue"

    def test_first_matching_rule_wins(self):
        r1 = DispatchRule("rule-a", lambda s: s.error_rate > 0.0, "queue-a")
        r2 = DispatchRule("rule-b", lambda s: s.error_rate > 0.0, "queue-b")
        s = make_snapshot("pipe-a", error_rate=0.2)
        result = dispatch_snapshots([s], [r1, r2])
        assert result.results[0].target == "queue-a"

    def test_multiple_snapshots_dispatched_independently(self):
        rule = DispatchRule("high-error", lambda s: s.error_rate > 0.3, "alert-queue")
        s1 = make_snapshot("pipe-a", error_rate=0.5)
        s2 = make_snapshot("pipe-b", error_rate=0.1)
        result = dispatch_snapshots([s1, s2], [rule], default_target="normal")
        targets = {r.pipeline_id: r.target for r in result.results}
        assert targets["pipe-a"] == "alert-queue"
        assert targets["pipe-b"] == "normal"

    def test_matched_filters_correctly(self):
        rule = DispatchRule("r", lambda s: s.error_rate > 0.3, "q")
        s1 = make_snapshot("pipe-a", error_rate=0.5)
        s2 = make_snapshot("pipe-b", error_rate=0.0)
        result = dispatch_snapshots([s1, s2], [rule])
        assert len(result.matched()) == 1
        assert result.matched()[0].pipeline_id == "pipe-a"

    def test_unmatched_filters_correctly(self):
        rule = DispatchRule("r", lambda s: s.error_rate > 0.3, "q")
        s1 = make_snapshot("pipe-a", error_rate=0.5)
        s2 = make_snapshot("pipe-b", error_rate=0.0)
        result = dispatch_snapshots([s1, s2], [rule])
        assert len(result.unmatched()) == 1
        assert result.unmatched()[0].pipeline_id == "pipe-b"

    def test_targets_groups_by_destination(self):
        r1 = DispatchRule("high", lambda s: s.error_rate > 0.3, "alert")
        r2 = DispatchRule("low", lambda s: s.throughput < 50, "slow")
        s1 = make_snapshot("pipe-a", error_rate=0.5)
        s2 = make_snapshot("pipe-b", error_rate=0.5)
        s3 = make_snapshot("pipe-c", throughput=10.0)
        result = dispatch_snapshots([s1, s2, s3], [r1, r2])
        groups = result.targets()
        assert set(groups["alert"]) == {"pipe-a", "pipe-b"}
        assert groups["slow"] == ["pipe-c"]

    def test_rule_exception_treated_as_no_match(self):
        bad_rule = DispatchRule("bad", lambda s: 1 / 0, "never")
        s = make_snapshot("pipe-a")
        result = dispatch_snapshots([s], [bad_rule], default_target="safe")
        assert result.results[0].target == "safe"
