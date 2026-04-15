"""Tests for checkpoint tracking."""
import pytest
from datetime import datetime
from pipewatch.checkpoint import Checkpoint, CheckpointStore


def make_checkpoint(pipeline_id="pipe-1", stage="extract", success=True, message=""):
    return Checkpoint(
        pipeline_id=pipeline_id,
        stage=stage,
        timestamp=datetime(2024, 1, 15, 10, 0, 0),
        success=success,
        message=message,
    )


class TestCheckpointStore:
    def test_empty_store_has_no_pipelines(self):
        store = CheckpointStore()
        assert store.pipeline_ids() == []

    def test_latest_returns_none_for_unknown_pipeline(self):
        store = CheckpointStore()
        assert store.latest("missing") is None

    def test_record_and_retrieve_latest(self):
        store = CheckpointStore()
        cp = make_checkpoint()
        store.record(cp)
        assert store.latest("pipe-1") is cp

    def test_latest_returns_most_recent(self):
        store = CheckpointStore()
        cp1 = make_checkpoint(stage="extract")
        cp2 = make_checkpoint(stage="transform")
        store.record(cp1)
        store.record(cp2)
        assert store.latest("pipe-1").stage == "transform"

    def test_history_returns_all_checkpoints(self):
        store = CheckpointStore()
        store.record(make_checkpoint(stage="extract"))
        store.record(make_checkpoint(stage="load"))
        assert len(store.history("pipe-1")) == 2

    def test_history_empty_for_unknown_pipeline(self):
        store = CheckpointStore()
        assert store.history("unknown") == []

    def test_last_failure_returns_none_when_all_pass(self):
        store = CheckpointStore()
        store.record(make_checkpoint(success=True))
        assert store.last_failure("pipe-1") is None

    def test_last_failure_returns_most_recent_failure(self):
        store = CheckpointStore()
        store.record(make_checkpoint(stage="extract", success=False, message="timeout"))
        store.record(make_checkpoint(stage="load", success=True))
        store.record(make_checkpoint(stage="validate", success=False, message="schema"))
        result = store.last_failure("pipe-1")
        assert result.stage == "validate"
        assert result.message == "schema"

    def test_all_passed_true_when_all_succeed(self):
        store = CheckpointStore()
        store.record(make_checkpoint(success=True))
        assert store.all_passed("pipe-1") is True

    def test_all_passed_false_when_any_fails(self):
        store = CheckpointStore()
        store.record(make_checkpoint(stage="extract", success=True))
        store.record(make_checkpoint(stage="load", success=False))
        assert store.all_passed("pipe-1") is False

    def test_all_passed_false_for_empty(self):
        store = CheckpointStore()
        assert store.all_passed("pipe-1") is False

    def test_str_includes_pipeline_and_stage(self):
        cp = make_checkpoint(pipeline_id="pipe-A", stage="load", success=False, message="err")
        s = str(cp)
        assert "pipe-A" in s
        assert "load" in s
        assert "FAIL" in s
        assert "err" in s
