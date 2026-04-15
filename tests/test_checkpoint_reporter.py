"""Tests for checkpoint reporter rendering."""
import pytest
from datetime import datetime
from pipewatch.checkpoint import Checkpoint, CheckpointStore
from pipewatch.checkpoint_reporter import (
    render_checkpoint_table,
    overall_checkpoint_status,
)


def make_checkpoint(pipeline_id="pipe-1", stage="extract", success=True, message=""):
    return Checkpoint(
        pipeline_id=pipeline_id,
        stage=stage,
        timestamp=datetime(2024, 6, 1, 12, 0, 0),
        success=success,
        message=message,
    )


class TestRenderCheckpointTable:
    def test_empty_store_returns_no_data_message(self):
        store = CheckpointStore()
        result = render_checkpoint_table(store)
        assert "No checkpoint data" in result

    def test_table_contains_pipeline_id(self):
        store = CheckpointStore()
        store.record(make_checkpoint(pipeline_id="sales-etl"))
        result = render_checkpoint_table(store)
        assert "sales-etl" in result

    def test_table_contains_stage_name(self):
        store = CheckpointStore()
        store.record(make_checkpoint(stage="transform"))
        result = render_checkpoint_table(store)
        assert "transform" in result

    def test_ok_shown_for_successful_checkpoint(self):
        store = CheckpointStore()
        store.record(make_checkpoint(success=True))
        result = render_checkpoint_table(store)
        assert "OK" in result

    def test_fail_shown_for_failed_checkpoint(self):
        store = CheckpointStore()
        store.record(make_checkpoint(success=False))
        result = render_checkpoint_table(store)
        assert "FAIL" in result

    def test_message_truncated_when_long(self):
        long_msg = "x" * 50
        store = CheckpointStore()
        store.record(make_checkpoint(message=long_msg))
        result = render_checkpoint_table(store)
        assert "..." in result

    def test_multiple_pipelines_all_shown(self):
        store = CheckpointStore()
        store.record(make_checkpoint(pipeline_id="pipe-A"))
        store.record(make_checkpoint(pipeline_id="pipe-B"))
        result = render_checkpoint_table(store)
        assert "pipe-A" in result
        assert "pipe-B" in result


class TestOverallCheckpointStatus:
    def test_unknown_when_empty(self):
        store = CheckpointStore()
        assert overall_checkpoint_status(store) == "UNKNOWN"

    def test_healthy_when_all_pass(self):
        store = CheckpointStore()
        store.record(make_checkpoint(success=True))
        assert overall_checkpoint_status(store) == "HEALTHY"

    def test_degraded_when_any_fail(self):
        store = CheckpointStore()
        store.record(make_checkpoint(pipeline_id="pipe-1", success=True))
        store.record(make_checkpoint(pipeline_id="pipe-2", success=False))
        assert overall_checkpoint_status(store) == "DEGRADED"
