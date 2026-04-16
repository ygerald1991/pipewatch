import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_tagger import TaggingRule, TaggingResult, apply_tagging_rules
from pipewatch.tagger_reporter import render_tagging_table, tagged_summary


def make_snapshot(pipeline_id: str, error_rate=0.0, throughput=100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status=None,
    )


class TestTaggingRule:
    def test_matches_when_no_constraints(self):
        rule = TaggingRule(tag="any")
        snap = make_snapshot("p1")
        assert rule.matches(snap)

    def test_min_error_rate_blocks_low_value(self):
        rule = TaggingRule(tag="high-error", min_error_rate=0.1)
        snap = make_snapshot("p1", error_rate=0.05)
        assert not rule.matches(snap)

    def test_min_error_rate_passes_equal_value(self):
        rule = TaggingRule(tag="high-error", min_error_rate=0.1)
        snap = make_snapshot("p1", error_rate=0.1)
        assert rule.matches(snap)

    def test_max_error_rate_blocks_high_value(self):
        rule = TaggingRule(tag="low-error", max_error_rate=0.05)
        snap = make_snapshot("p1", error_rate=0.2)
        assert not rule.matches(snap)

    def test_min_throughput_blocks_low_value(self):
        rule = TaggingRule(tag="high-tp", min_throughput=200.0)
        snap = make_snapshot("p1", throughput=50.0)
        assert not rule.matches(snap)

    def test_max_throughput_blocks_high_value(self):
        rule = TaggingRule(tag="low-tp", max_throughput=50.0)
        snap = make_snapshot("p1", throughput=200.0)
        assert not rule.matches(snap)

    def test_combined_constraints_all_pass(self):
        rule = TaggingRule(tag="mid", min_error_rate=0.01, max_error_rate=0.1, min_throughput=50.0)
        snap = make_snapshot("p1", error_rate=0.05, throughput=100.0)
        assert rule.matches(snap)


class TestApplyTaggingRules:
    def test_empty_snapshots_returns_empty(self):
        rules = [TaggingRule(tag="any")]
        assert apply_tagging_rules([], rules) == []

    def test_no_rules_returns_empty_tags(self):
        snaps = [make_snapshot("p1")]
        results = apply_tagging_rules(snaps, [])
        assert results[0].tags == []

    def test_matching_rule_assigns_tag(self):
        snaps = [make_snapshot("p1", error_rate=0.5)]
        rules = [TaggingRule(tag="critical", min_error_rate=0.3)]
        results = apply_tagging_rules(snaps, rules)
        assert "critical" in results[0].tags

    def test_multiple_tags_can_match(self):
        snaps = [make_snapshot("p1", error_rate=0.5, throughput=10.0)]
        rules = [
            TaggingRule(tag="high-error", min_error_rate=0.3),
            TaggingRule(tag="low-throughput", max_throughput=50.0),
        ]
        results = apply_tagging_rules(snaps, rules)
        assert set(results[0].tags) == {"high-error", "low-throughput"}

    def test_pipeline_id_preserved(self):
        snaps = [make_snapshot("my-pipeline")]
        results = apply_tagging_rules(snaps, [])
        assert results[0].pipeline_id == "my-pipeline"


class TestTaggerReporter:
    def test_empty_returns_no_pipelines_message(self):
        assert "No pipelines" in render_tagging_table([])

    def test_table_contains_pipeline_id(self):
        results = [TaggingResult(pipeline_id="pipe-a", tags=["ok"])]
        assert "pipe-a" in render_tagging_table(results)

    def test_table_contains_tag(self):
        results = [TaggingResult(pipeline_id="pipe-a", tags=["critical"])]
        assert "critical" in render_tagging_table(results)

    def test_tagged_summary_groups_by_tag(self):
        results = [
            TaggingResult(pipeline_id="p1", tags=["slow"]),
            TaggingResult(pipeline_id="p2", tags=["slow"]),
        ]
        summary = tagged_summary(results)
        assert "slow" in summary
        assert "p1" in summary
        assert "p2" in summary

    def test_tagged_summary_no_matches(self):
        results = [TaggingResult(pipeline_id="p1", tags=[])]
        assert "No tags matched" in tagged_summary(results)
