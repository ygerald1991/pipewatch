from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_scheduler import PipelineScheduler, ScheduleEntry


def make_scheduler(*pipeline_ids: str, interval: int = 60) -> PipelineScheduler:
    s = PipelineScheduler()
    for pid in pipeline_ids:
        s.register(pid, interval)
    return s


class TestScheduleEntry:
    def test_due_when_never_run(self):
        entry = ScheduleEntry(pipeline_id="p1", interval_seconds=30)
        assert entry.is_due() is True

    def test_not_due_immediately_after_run(self):
        now = datetime.utcnow()
        entry = ScheduleEntry(pipeline_id="p1", interval_seconds=60, last_run=now)
        assert entry.is_due(now) is False

    def test_due_after_interval_elapsed(self):
        past = datetime.utcnow() - timedelta(seconds=61)
        entry = ScheduleEntry(pipeline_id="p1", interval_seconds=60, last_run=past)
        assert entry.is_due() is True

    def test_not_due_when_disabled(self):
        entry = ScheduleEntry(pipeline_id="p1", interval_seconds=1, enabled=False)
        assert entry.is_due() is False

    def test_mark_run_updates_last_run(self):
        entry = ScheduleEntry(pipeline_id="p1", interval_seconds=60)
        now = datetime.utcnow()
        entry.mark_run(now)
        assert entry.last_run == now

    def test_str_representation(self):
        entry = ScheduleEntry(pipeline_id="pipe-a", interval_seconds=120)
        s = str(entry)
        assert "pipe-a" in s
        assert "120s" in s
        assert "never" in s


class TestPipelineScheduler:
    def test_register_creates_entry(self):
        s = make_scheduler("p1")
        assert "p1" in s.pipeline_ids()

    def test_register_is_idempotent(self):
        s = make_scheduler("p1", "p1")
        assert s.pipeline_ids().count("p1") == 1

    def test_due_pipelines_returns_unrun(self):
        s = make_scheduler("p1", "p2")
        due = s.due_pipelines()
        assert "p1" in due
        assert "p2" in due

    def test_tick_marks_pipelines_triggered(self):
        s = make_scheduler("p1", "p2")
        result = s.tick()
        assert "p1" in result.triggered
        assert "p2" in result.triggered
        assert result.skipped == []

    def test_tick_skips_recently_run(self):
        now = datetime.utcnow()
        s = make_scheduler("p1", interval=300)
        s.tick(now)  # mark as run
        result = s.tick(now + timedelta(seconds=10))
        assert "p1" in result.skipped
        assert "p1" not in result.triggered

    def test_disable_prevents_triggering(self):
        s = make_scheduler("p1")
        s.disable("p1")
        result = s.tick()
        assert "p1" not in result.triggered

    def test_enable_restores_triggering(self):
        s = make_scheduler("p1")
        s.disable("p1")
        s.enable("p1")
        result = s.tick()
        assert "p1" in result.triggered

    def test_entry_for_returns_none_for_unknown(self):
        s = make_scheduler()
        assert s.entry_for("unknown") is None

    def test_total_in_result(self):
        s = make_scheduler("p1", "p2", "p3")
        result = s.tick()
        assert result.total == 3
