import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_auditor_session import AuditorSession
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestAuditorSession:
    def test_empty_session_has_no_pipeline_ids(self):
        session = AuditorSession()
        assert session.pipeline_ids == []

    def test_add_snapshot_registers_pipeline(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        assert "pipe-1" in session.pipeline_ids

    def test_run_returns_audit_log(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        log = session.run()
        assert log is not None

    def test_healthy_pipeline_records_healthy_event(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-ok", error_rate=0.01))
        log = session.run()
        events = log.events_for("pipe-ok")
        assert any(e.event_type == "healthy" for e in events)

    def test_high_error_rate_records_warning_event(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-warn", error_rate=0.15))
        log = session.run()
        events = log.events_for("pipe-warn")
        assert any(e.event_type == "high_error_rate" for e in events)

    def test_critical_error_rate_records_critical_event(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-crit", error_rate=0.25))
        log = session.run()
        events = log.events_for("pipe-crit")
        assert any(e.event_type == "critical_error_rate" for e in events)

    def test_multiple_snapshots_all_recorded(self):
        session = AuditorSession()
        session.add_snapshot(make_snapshot("pipe-a", error_rate=0.0))
        session.add_snapshot(make_snapshot("pipe-b", error_rate=0.5))
        log = session.run()
        assert len(log.events_for("pipe-a")) == 1
        assert len(log.events_for("pipe-b")) == 1

    def test_manual_record_event(self):
        session = AuditorSession()
        event = session.record_event("pipe-x", "manual", "manually recorded")
        assert event.pipeline_id == "pipe-x"
        assert event.event_type == "manual"
        assert event.message == "manually recorded"
