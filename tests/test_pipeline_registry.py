"""Tests for PipelineRegistry."""

import pytest
from datetime import datetime

from pipewatch.pipeline_registry import PipelineRegistry
from pipewatch.metrics import PipelineMetric


def make_metric(pipeline_id="pipe-1", processed=100, failed=2):
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        records_processed=processed,
        records_failed=failed,
        duration_seconds=60.0,
    )


class TestPipelineRegistry:
    def test_register_creates_collector(self):
        reg = PipelineRegistry()
        collector = reg.register("pipe-1")
        assert collector is not None

    def test_register_is_idempotent(self):
        reg = PipelineRegistry()
        c1 = reg.register("pipe-1")
        c2 = reg.register("pipe-1")
        assert c1 is c2

    def test_record_auto_registers_pipeline(self):
        reg = PipelineRegistry()
        reg.record(make_metric("pipe-x"))
        assert "pipe-x" in reg

    def test_collector_for_returns_none_for_unknown(self):
        reg = PipelineRegistry()
        assert reg.collector_for("unknown") is None

    def test_collector_for_returns_collector_after_register(self):
        reg = PipelineRegistry()
        reg.register("pipe-1")
        assert reg.collector_for("pipe-1") is not None

    def test_pipeline_ids_lists_all(self):
        reg = PipelineRegistry()
        reg.register("a")
        reg.register("b")
        assert set(reg.pipeline_ids()) == {"a", "b"}

    def test_len_reflects_registered_count(self):
        reg = PipelineRegistry()
        reg.register("a")
        reg.register("b")
        assert len(reg) == 2

    def test_record_stores_metric_in_collector(self):
        reg = PipelineRegistry()
        m = make_metric("pipe-1")
        reg.record(m)
        collector = reg.collector_for("pipe-1")
        assert collector is not None
        assert collector.latest("pipe-1") == m
