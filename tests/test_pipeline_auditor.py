"""Tests for pipewatch.pipeline_auditor."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_auditor import AuditEvent, AuditLog, audit_snapshot_change


def make_snapshot(pipeline_id="pipe-1", status=PipelineStatus.HEALTHY,
                  error_rate=0.01, throughput=100.0, metric_count=5):
    snap = MagicMock()
    snap.pipeline_id = pipeline_id
    snap.status = status
    snap.error_rate = error_rate
    snap.throughput = throughput
    snap.metric_count = metric_count
    return snap


class TestAuditLog:
    def test_new_log_is_empty(self):
        log = AuditLog()
        assert len(log) == 0

    def test_record_adds_event(self):
        log = AuditLog()
        event = AuditEvent(pipeline_id="p1", event_type="TEST", detail="hello")
        log.record(event)
        assert len(log) == 1

    def test_events_for_filters_by_pipeline(self):
        log = AuditLog()
        log.record(AuditEvent("p1", "TYPE_A", "detail"))
        log.record(AuditEvent("p2", "TYPE_B", "detail"))
        log.record(AuditEvent("p1", "TYPE_C", "detail"))
        assert len(log.events_for("p1")) == 2
        assert len(log.events_for("p2")) == 1

    def test_all_events_returns_all(self):
        log = AuditLog()
        log.record(AuditEvent("p1", "A", "d"))
        log.record(AuditEvent("p2", "B", "d"))
        assert len(log.all_events()) == 2

    def test_clear_removes_all_events(self):
        log = AuditLog()
        log.record(AuditEvent("p1", "A", "d"))
        log.clear()
        assert len(log) == 0

    def test_audit_event_str_contains_pipeline_id(self):
        event = AuditEvent("pipe-42", "STATUS_CHANGE", "went critical",
                           timestamp=datetime(2024, 1, 15, 12, 0, 0))
        result = str(event)
        assert "pipe-42" in result
        assert "STATUS_CHANGE" in result
        assert "went critical" in result


class TestAuditSnapshotChange:
    def test_first_snapshot_creates_created_event(self):
        log = AuditLog()
        snap = make_snapshot()
        audit_snapshot_change(log, None, snap)
        events = log.events_for("pipe-1")
        assert len(events) == 1
        assert events[0].event_type == "SNAPSHOT_CREATED"

    def test_no_event_when_nothing_changes(self):
        log = AuditLog()
        prev = make_snapshot()
        curr = make_snapshot()
        audit_snapshot_change(log, prev, curr)
        assert len(log) == 0

    def test_status_change_recorded(self):
        log = AuditLog()
        prev = make_snapshot(status=PipelineStatus.HEALTHY)
        curr = make_snapshot(status=PipelineStatus.CRITICAL)
        audit_snapshot_change(log, prev, curr)
        types = [e.event_type for e in log.all_events()]
        assert "STATUS_CHANGE" in types

    def test_large_error_rate_shift_recorded(self):
        log = AuditLog()
        prev = make_snapshot(error_rate=0.02)
        curr = make_snapshot(error_rate=0.10)
        audit_snapshot_change(log, prev, curr)
        types = [e.event_type for e in log.all_events()]
        assert "ERROR_RATE_SHIFT" in types

    def test_small_error_rate_shift_not_recorded(self):
        log = AuditLog()
        prev = make_snapshot(error_rate=0.02)
        curr = make_snapshot(error_rate=0.03)
        audit_snapshot_change(log, prev, curr)
        types = [e.event_type for e in log.all_events()]
        assert "ERROR_RATE_SHIFT" not in types

    def test_large_throughput_shift_recorded(self):
        log = AuditLog()
        prev = make_snapshot(throughput=100.0)
        curr = make_snapshot(throughput=50.0)
        audit_snapshot_change(log, prev, curr)
        types = [e.event_type for e in log.all_events()]
        assert "THROUGHPUT_SHIFT" in types

    def test_small_throughput_shift_not_recorded(self):
        log = AuditLog()
        prev = make_snapshot(throughput=100.0)
        curr = make_snapshot(throughput=105.0)
        audit_snapshot_change(log, prev, curr)
        types = [e.event_type for e in log.all_events()]
        assert "THROUGHPUT_SHIFT" not in types
