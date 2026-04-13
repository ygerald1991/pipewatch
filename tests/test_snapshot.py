"""Tests for pipewatch.snapshot module."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.snapshot import (
    PipelineSnapshot,
    SnapshotStore,
    capture_snapshot,
)


def make_metric(
    pipeline_id="pipe-1",
    processed=100,
    failed=5,
    duration=10.0,
    timestamp=None,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        processed=processed,
        failed=failed,
        duration_seconds=duration,
        timestamp=timestamp or datetime.utcnow(),
    )


class TestCaptureSnapshot:
    def test_returns_none_for_empty_metrics(self):
        result = capture_snapshot("pipe-1", [])
        assert result is None

    def test_returns_snapshot_with_correct_pipeline_id(self):
        metrics = [make_metric()]
        snap = capture_snapshot("pipe-1", metrics)
        assert snap is not None
        assert snap.pipeline_id == "pipe-1"

    def test_snapshot_metric_count_matches(self):
        metrics = [make_metric(), make_metric(), make_metric()]
        snap = capture_snapshot("pipe-1", metrics)
        assert snap.metric_count == 3

    def test_snapshot_has_valid_status(self):
        metrics = [make_metric(failed=0)]
        snap = capture_snapshot("pipe-1", metrics)
        assert isinstance(snap.status, PipelineStatus)

    def test_snapshot_to_dict_contains_expected_keys(self):
        snap = capture_snapshot("pipe-1", [make_metric()])
        d = snap.to_dict()
        assert set(d.keys()) == {
            "pipeline_id",
            "captured_at",
            "status",
            "error_rate",
            "throughput",
            "metric_count",
        }


class TestPipelineSnapshotDegradation:
    def _make_snap(self, status: PipelineStatus) -> PipelineSnapshot:
        return PipelineSnapshot(
            pipeline_id="pipe-1",
            captured_at=datetime.utcnow(),
            status=status,
            error_rate=0.0,
            throughput=10.0,
            metric_count=1,
        )

    def test_degraded_when_status_worsens(self):
        healthy = self._make_snap(PipelineStatus.HEALTHY)
        warning = self._make_snap(PipelineStatus.WARNING)
        assert warning.is_degraded_compared_to(healthy)

    def test_not_degraded_when_status_improves(self):
        healthy = self._make_snap(PipelineStatus.HEALTHY)
        warning = self._make_snap(PipelineStatus.WARNING)
        assert not healthy.is_degraded_compared_to(warning)


class TestSnapshotStore:
    def _make_snap(self, pipeline_id="pipe-1", status=PipelineStatus.HEALTHY):
        return PipelineSnapshot(
            pipeline_id=pipeline_id,
            captured_at=datetime.utcnow(),
            status=status,
            error_rate=0.0,
            throughput=10.0,
            metric_count=1,
        )

    def test_get_returns_none_before_update(self):
        store = SnapshotStore()
        assert store.get("pipe-1") is None

    def test_update_stores_snapshot(self):
        store = SnapshotStore()
        snap = self._make_snap()
        store.update(snap)
        assert store.get("pipe-1") is snap

    def test_first_update_returns_false(self):
        store = SnapshotStore()
        changed = store.update(self._make_snap())
        assert changed is False

    def test_update_returns_true_on_status_change(self):
        store = SnapshotStore()
        store.update(self._make_snap(status=PipelineStatus.HEALTHY))
        changed = store.update(self._make_snap(status=PipelineStatus.WARNING))
        assert changed is True

    def test_update_returns_false_on_same_status(self):
        store = SnapshotStore()
        store.update(self._make_snap(status=PipelineStatus.HEALTHY))
        changed = store.update(self._make_snap(status=PipelineStatus.HEALTHY))
        assert changed is False

    def test_all_returns_all_snapshots(self):
        store = SnapshotStore()
        store.update(self._make_snap("pipe-1"))
        store.update(self._make_snap("pipe-2"))
        assert len(store.all()) == 2
