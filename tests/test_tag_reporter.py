"""Tests for pipewatch.tag_reporter."""
import pytest
from pipewatch.tag_filter import TagRegistry
from pipewatch.tag_reporter import (
    render_tag_table,
    pipelines_for_tag_summary,
)


class TestRenderTagTable:
    def test_empty_registry_returns_no_tags_message(self):
        reg = TagRegistry()
        assert render_tag_table(reg) == "No tags registered."

    def test_output_contains_tag_name(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        output = render_tag_table(reg)
        assert "prod" in output

    def test_output_contains_pipeline_id(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        output = render_tag_table(reg)
        assert "pipe-a" in output

    def test_multiple_pipelines_same_tag(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "prod")
        output = render_tag_table(reg)
        assert "pipe-a" in output
        assert "pipe-b" in output

    def test_multiple_tags_each_shown(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "staging")
        output = render_tag_table(reg)
        assert "prod" in output
        assert "staging" in output

    def test_header_present(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        output = render_tag_table(reg)
        assert "TAG" in output
        assert "PIPELINES" in output


class TestPipelinesForTagSummary:
    def test_unknown_tag_returns_no_pipelines_message(self):
        reg = TagRegistry()
        result = pipelines_for_tag_summary(reg, "prod")
        assert "No pipelines" in result
        assert "prod" in result

    def test_known_tag_lists_pipelines(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "prod")
        reg.tag("pipe-b", "prod")
        result = pipelines_for_tag_summary(reg, "prod")
        assert "pipe-a" in result
        assert "pipe-b" in result

    def test_summary_includes_tag_name(self):
        reg = TagRegistry()
        reg.tag("pipe-a", "critical")
        result = pipelines_for_tag_summary(reg, "critical")
        assert "critical" in result
