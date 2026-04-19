import pytest
from datetime import datetime

from pipewatch.pipeline_acknowledger import AckEntry, AcknowledgerResult
from pipewatch.acknowledger_reporter import render_acknowledger_table, acknowledged_summary


def make_entry(pipeline_id: str, by: str = "alice", reason: str = "") -> AckEntry:
    return AckEntry(
        pipeline_id=pipeline_id,
        acknowledged_at=datetime(2024, 1, 15, 10, 30, 0),
        acknowledged_by=by,
        reason=reason,
    )


class TestRenderAcknowledgerTable:
    def test_empty_returns_no_pipelines_message(self):
        result = AcknowledgerResult(entries=[])
        output = render_acknowledger_table(result)
        assert "No acknowledged" in output

    def test_output_contains_pipeline_id(self):
        result = AcknowledgerResult(entries=[make_entry("pipe-1")])
        output = render_acknowledger_table(result)
        assert "pipe-1" in output

    def test_output_contains_acknowledged_by(self):
        result = AcknowledgerResult(entries=[make_entry("pipe-1", by="bob")])
        output = render_acknowledger_table(result)
        assert "bob" in output

    def test_output_contains_reason(self):
        result = AcknowledgerResult(entries=[make_entry("pipe-1", reason="planned downtime")])
        output = render_acknowledger_table(result)
        assert "planned downtime" in output

    def test_multiple_entries_all_shown(self):
        entries = [make_entry("pipe-1"), make_entry("pipe-2")]
        result = AcknowledgerResult(entries=entries)
        output = render_acknowledger_table(result)
        assert "pipe-1" in output
        assert "pipe-2" in output

    def test_summary_empty(self):
        result = AcknowledgerResult(entries=[])
        summary = acknowledged_summary(result)
        assert "No pipelines" in summary

    def test_summary_lists_pipeline_ids(self):
        result = AcknowledgerResult(entries=[make_entry("pipe-1"), make_entry("pipe-2")])
        summary = acknowledged_summary(result)
        assert "pipe-1" in summary
        assert "pipe-2" in summary
        assert "2" in summary
