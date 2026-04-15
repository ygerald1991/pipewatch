from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.scheduler_session import SchedulerSession


class TestSchedulerSession:
    def test_register_adds_pipeline(self):
        session = SchedulerSession()
        session.register("pipe-1")
        assert "pipe-1" in session.pipeline_ids()

    def test_register_many_adds_all(self):
        session = SchedulerSession()
        session.register_many(["a", "b", "c"])
        ids = session.pipeline_ids()
        assert "a" in ids
        assert "b" in ids
        assert "c" in ids

    def test_default_interval_used_when_not_specified(self):
        session = SchedulerSession(default_interval_seconds=120)
        session.register("p1")
        entry = session.scheduler.entry_for("p1")
        assert entry is not None
        assert entry.interval_seconds == 120

    def test_custom_interval_overrides_default(self):
        session = SchedulerSession(default_interval_seconds=60)
        session.register("p1", interval_seconds=30)
        entry = session.scheduler.entry_for("p1")
        assert entry is not None
        assert entry.interval_seconds == 30

    def test_tick_returns_result(self):
        session = SchedulerSession()
        session.register("p1")
        result = session.tick()
        assert "p1" in result.triggered

    def test_tick_history_is_recorded(self):
        session = SchedulerSession()
        session.register("p1")
        session.tick()
        session.tick()
        assert len(session.history()) == 2

    def test_total_triggered_accumulates(self):
        now = datetime.utcnow()
        session = SchedulerSession(default_interval_seconds=1)
        session.register("p1")
        session.tick(now)
        session.tick(now + timedelta(seconds=2))
        assert session.total_triggered() == 2

    def test_disable_prevents_trigger(self):
        session = SchedulerSession()
        session.register("p1")
        session.disable("p1")
        result = session.tick()
        assert "p1" not in result.triggered

    def test_enable_after_disable_restores(self):
        session = SchedulerSession()
        session.register("p1")
        session.disable("p1")
        session.enable("p1")
        result = session.tick()
        assert "p1" in result.triggered

    def test_due_pipelines_reflects_scheduler_state(self):
        session = SchedulerSession()
        session.register("p1")
        session.register("p2")
        due = session.due_pipelines()
        assert "p1" in due
        assert "p2" in due
