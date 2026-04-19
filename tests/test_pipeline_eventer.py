from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_eventer import (
    PipelineEvent,
    EventerResult,
    emit_event,
    emit_events_from_snapshots,
)


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=error_rate,
        throughput=100.0,
        metric_count=5,
    )


class TestEmitEvent:
    def test_event_has_correct_pipeline_id(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "STATUS_CHANGE", "Pipeline went healthy")
        assert event.pipeline_id == "pipe-1"

    def test_event_has_correct_type(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "ALERT", "High error rate")
        assert event.event_type == "ALERT"

    def test_event_has_correct_message(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "INFO", "All good")
        assert event.message == "All good"

    def test_event_metadata_defaults_to_empty(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "INFO", "msg")
        assert event.metadata == {}

    def test_event_metadata_is_stored(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "INFO", "msg", metadata={"key": "val"})
        assert event.metadata["key"] == "val"

    def test_event_str_contains_pipeline_id(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "INFO", "msg")
        assert "pipe-1" in str(event)

    def test_event_timestamp_is_set(self):
        s = make_snapshot("pipe-1")
        event = emit_event(s, "INFO", "msg")
        assert isinstance(event.timestamp, datetime)


class TestEmitEventsFromSnapshots:
    def test_empty_snapshots_returns_empty(self):
        result = emit_events_from_snapshots([], "INFO")
        assert result.total == 0

    def test_single_snapshot_produces_one_event(self):
        result = emit_events_from_snapshots([make_snapshot("p1")], "INFO")
        assert result.total == 1

    def test_pipeline_ids_registered(self):
        snapshots = [make_snapshot("p1"), make_snapshot("p2")]
        result = emit_events_from_snapshots(snapshots, "INFO")
        assert set(result.pipeline_ids()) == {"p1", "p2"}

    def test_events_for_filters_correctly(self):
        snapshots = [make_snapshot("p1"), make_snapshot("p2")]
        result = emit_events_from_snapshots(snapshots, "INFO")
        assert all(e.pipeline_id == "p1" for e in result.events_for("p1"))

    def test_of_type_filters_correctly(self):
        snapshots = [make_snapshot("p1")]
        result = emit_events_from_snapshots(snapshots, "ALERT")
        assert len(result.of_type("ALERT")) == 1
        assert len(result.of_type("INFO")) == 0

    def test_custom_message_fn_applied(self):
        snapshots = [make_snapshot("p1")]
        result = emit_events_from_snapshots(
            snapshots, "INFO", message_fn=lambda s: f"custom-{s.pipeline_id}"
        )
        assert result.events[0].message == "custom-p1"
