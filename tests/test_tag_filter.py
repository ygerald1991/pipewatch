"""Tests for pipewatch.tag_filter."""
import pytest
from pipewatch.tag_filter import TagFilter, TagRegistry, filter_metrics
from pipewatch.metrics import PipelineMetric
import time


def make_metric(pipeline_id: str) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=time.time(),
        records_processed=100,
        records_failed=0,
        duration_seconds=10.0,
    )


class TestTagFilter:
    def test_empty_filter_matches_any_tags(self):
        f = TagFilter()
        assert f.matches(["prod", "critical"]) is True

    def test_required_tag_present(self):
        f = TagFilter(required={"prod"})
        assert f.matches(["prod", "etl"]) is True

    def test_required_tag_missing(self):
        f = TagFilter(required={"prod"})
        assert f.matches(["staging"]) is False

    def test_excluded_tag_absent(self):
        f = TagFilter(excluded={"deprecated"})
        assert f.matches(["prod"]) is True

    def test_excluded_tag_present(self):
        f = TagFilter(excluded={"deprecated"})
        assert f.matches(["prod", "deprecated"]) is False

    def test_required_and_excluded_combined(self):
        f = TagFilter(required={"prod"}, excluded={"paused"})
        assert f.matches(["prod"]) is True
        assert f.matches(["prod", "paused"]) is False
        assert f.matches(["staging"]) is False


class TestTagRegistry:
    def test_tags_for_unknown_pipeline_is_empty(self):
        reg = TagRegistry()
        assert reg.tags_for("pipe-x") == set()

    def test_tag_and_retrieve(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod", "critical")
        assert reg.tags_for("pipe-a") == {"prod", "critical"}

    def test_tag_accumulates(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-a", "critical")
        assert "prod" in reg.tags_for("pipe-a")
        assert "critical" in reg.tags_for("pipe-a")

    def test_filter_pipelines_returns_matches(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "staging")
        result = reg.filter_pipelines(TagFilter(required={"prod"}))
        assert result == ["pipe-a"]

    def test_filter_pipelines_with_candidates(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "prod")
        result = reg.filter_pipelines(
            TagFilter(required={"prod"}), candidates=["pipe-a"]
        )
        assert result == ["pipe-a"]


class TestFilterMetrics:
    def test_returns_only_matching_metrics(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "staging")
        metrics = [make_metric("pipe-a"), make_metric("pipe-b")]
        result = filter_metrics(metrics, reg, TagFilter(required={"prod"}))
        assert len(result) == 1
        assert result[0].pipeline_id == "pipe-a"

    def test_returns_empty_when_no_match(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "staging")
        metrics = [make_metric("pipe-a")]
        result = filter_metrics(metrics, reg, TagFilter(required={"prod"}))
        assert result == []
