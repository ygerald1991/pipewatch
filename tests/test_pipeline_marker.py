import pytest
from datetime import datetime
from pipewatch.pipeline_marker import PipelineMarker, MarkEntry, MarkerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        timestamp=datetime.utcnow(),
        error_rate=error_rate,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=5,
    )


class TestPipelineMarker:
    def setup_method(self):
        self.marker = PipelineMarker()

    def test_new_marker_has_no_marks(self):
        assert self.marker.marked_pipeline_ids == []

    def test_mark_returns_entry(self):
        snap = make_snapshot("pipe-1")
        entry = self.marker.mark(snap, "review", "high error rate")
        assert isinstance(entry, MarkEntry)
        assert entry.pipeline_id == "pipe-1"
        assert entry.label == "review"
        assert entry.reason == "high error rate"

    def test_is_marked_returns_true_after_mark(self):
        snap = make_snapshot("pipe-1")
        self.marker.mark(snap, "flagged", "anomaly detected")
        assert self.marker.is_marked("pipe-1")

    def test_is_marked_returns_false_for_unknown(self):
        assert not self.marker.is_marked("unknown")

    def test_get_mark_returns_entry(self):
        snap = make_snapshot("pipe-2")
        self.marker.mark(snap, "critical", "sla breach")
        entry = self.marker.get_mark("pipe-2")
        assert entry is not None
        assert entry.label == "critical"

    def test_get_mark_returns_none_for_unknown(self):
        assert self.marker.get_mark("missing") is None

    def test_unmark_removes_entry(self):
        snap = make_snapshot("pipe-3")
        self.marker.mark(snap, "warn", "slow throughput")
        result = self.marker.unmark("pipe-3")
        assert result is True
        assert not self.marker.is_marked("pipe-3")

    def test_unmark_returns_false_for_unknown(self):
        assert self.marker.unmark("nonexistent") is False

    def test_all_marks_returns_marker_result(self):
        snap1 = make_snapshot("pipe-a")
        snap2 = make_snapshot("pipe-b")
        self.marker.mark(snap1, "review", "reason a")
        self.marker.mark(snap2, "critical", "reason b")
        result = self.marker.all_marks()
        assert isinstance(result, MarkerResult)
        assert result.total_marked == 2

    def test_for_label_filters_correctly(self):
        snap1 = make_snapshot("pipe-x")
        snap2 = make_snapshot("pipe-y")
        self.marker.mark(snap1, "review", "r1")
        self.marker.mark(snap2, "critical", "r2")
        review_entries = self.marker.all_marks().for_label("review")
        assert len(review_entries) == 1
        assert review_entries[0].pipeline_id == "pipe-x"

    def test_mark_str_representation(self):
        snap = make_snapshot("pipe-z")
        entry = self.marker.mark(snap, "warn", "test reason")
        assert "pipe-z" in str(entry)
        assert "warn" in str(entry)
        assert "test reason" in str(entry)
