"""Tests for pipewatch.baseline_store."""

import pytest
from pipewatch.baseline_store import BaselineStore
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id="pipe-1"):
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=0.01,
        throughput=50.0,
        metric_count=5,
        status=PipelineStatus.HEALTHY,
    )


class TestBaselineStore:
    def test_empty_store_has_no_pipelines(self):
        store = BaselineStore()
        assert len(store) == 0

    def test_set_and_get_baseline(self):
        store = BaselineStore()
        snap = make_snapshot("pipe-a")
        store.set(snap)
        assert store.get("pipe-a") is snap

    def test_get_missing_returns_none(self):
        store = BaselineStore()
        assert store.get("nonexistent") is None

    def test_contains_after_set(self):
        store = BaselineStore()
        store.set(make_snapshot("pipe-x"))
        assert "pipe-x" in store

    def test_not_contains_before_set(self):
        store = BaselineStore()
        assert "pipe-x" not in store

    def test_remove_existing(self):
        store = BaselineStore()
        store.set(make_snapshot("pipe-b"))
        store.remove("pipe-b")
        assert store.get("pipe-b") is None
        assert len(store) == 0

    def test_remove_nonexistent_does_not_raise(self):
        store = BaselineStore()
        store.remove("ghost")  # should not raise

    def test_pipeline_ids_lists_all_keys(self):
        store = BaselineStore()
        store.set(make_snapshot("p1"))
        store.set(make_snapshot("p2"))
        assert set(store.pipeline_ids()) == {"p1", "p2"}

    def test_overwrite_updates_baseline(self):
        store = BaselineStore()
        snap1 = make_snapshot("pipe-c")
        snap2 = PipelineSnapshot(
            pipeline_id="pipe-c",
            error_rate=0.99,
            throughput=1.0,
            metric_count=1,
            status=PipelineStatus.CRITICAL,
        )
        store.set(snap1)
        store.set(snap2)
        assert store.get("pipe-c") is snap2
        assert len(store) == 1
