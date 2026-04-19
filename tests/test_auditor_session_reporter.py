import pytest
from datetime import datetime
from pipewatch.pipeline_auditor import AuditLog
from pipewatch.auditor_session_reporter import render_audit_table, event_type_summary


class TestRenderAuditTable:
    def test_empty_log_returns_no_events_message(self):
        log = AuditLog()
        result = render_audit_table(log)
        assert "no events" in result

    def test_output_contains_pipeline_id(self):
        log = AuditLog()
        log.record("pipe-alpha", "healthy", "all good")
        result = render_audit_table(log)
        assert "pipe-alpha" in result

    def test_output_contains_event_type(self):
        log = AuditLog()
        log.record("pipe-alpha", "high_error_rate", "too many errors")
        result = render_audit_table(log)
        assert "high_error_rate" in result

    def test_output_contains_message(self):
        log = AuditLog()
        log.record("pipe-alpha", "healthy", "operating normally")
        result = render_audit_table(log)
        assert "operating normally" in result

    def test_filter_by_pipeline_id(self):
        log = AuditLog()
        log.record("pipe-a", "healthy", "ok")
        log.record("pipe-b", "critical_error_rate", "bad")
        result = render_audit_table(log, pipeline_id="pipe-a")
        assert "pipe-a" in result
        assert "pipe-b" not in result

    def test_total_events_shown(self):
        log = AuditLog()
        log.record("pipe-a", "healthy", "ok")
        log.record("pipe-a", "healthy", "still ok")
        result = render_audit_table(log)
        assert "Total events: 2" in result


class TestEventTypeSummary:
    def test_empty_log_returns_no_events(self):
        log = AuditLog()
        result = event_type_summary(log)
        assert "No events" in result

    def test_counts_event_types(self):
        log = AuditLog()
        log.record("pipe-a", "healthy", "ok")
        log.record("pipe-b", "healthy", "ok")
        log.record("pipe-c", "critical_error_rate", "bad")
        result = event_type_summary(log)
        assert "healthy" in result
        assert "critical_error_rate" in result
