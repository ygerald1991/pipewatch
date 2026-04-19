from datetime import datetime, timezone, timedelta
import pytest
from pipewatch.pipeline_heartbeat import PipelineHeartbeat, HeartbeatEntry


NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class TestPipelineHeartbeat:
    def setup_method(self):
        self.hb = PipelineHeartbeat(default_interval=30.0)

    def test_new_heartbeat_has_no_pipelines(self):
        assert self.hb.pipeline_ids() == []

    def test_beat_registers_pipeline(self):
        self.hb.beat("pipe-a", now=NOW)
        assert "pipe-a" in self.hb.pipeline_ids()

    def test_latest_returns_none_for_unknown(self):
        assert self.hb.latest("missing") is None

    def test_latest_returns_entry_after_beat(self):
        self.hb.beat("pipe-a", now=NOW)
        entry = self.hb.latest("pipe-a")
        assert entry is not None
        assert entry.pipeline_id == "pipe-a"
        assert entry.last_seen == NOW

    def test_alive_within_interval(self):
        self.hb.beat("pipe-a", now=NOW)
        check_time = NOW + timedelta(seconds=20)
        result = self.hb.check(now=check_time)
        assert result.total_alive == 1
        assert result.total_dead == 0
        assert result.all_alive

    def test_dead_after_interval_exceeded(self):
        self.hb.beat("pipe-a", now=NOW)
        check_time = NOW + timedelta(seconds=60)
        result = self.hb.check(now=check_time)
        assert result.total_dead == 1
        assert result.total_alive == 0
        assert not result.all_alive

    def test_custom_interval_overrides_default(self):
        self.hb.set_interval("pipe-a", 120.0)
        self.hb.beat("pipe-a", now=NOW)
        check_time = NOW + timedelta(seconds=90)
        result = self.hb.check(now=check_time)
        assert result.total_alive == 1

    def test_multiple_pipelines_mixed_status(self):
        self.hb.beat("pipe-alive", now=NOW)
        self.hb.beat("pipe-dead", now=NOW - timedelta(seconds=100))
        result = self.hb.check(now=NOW)
        assert result.total_alive == 1
        assert result.total_dead == 1

    def test_beat_updates_existing_entry(self):
        self.hb.beat("pipe-a", now=NOW)
        later = NOW + timedelta(seconds=10)
        self.hb.beat("pipe-a", now=later)
        entry = self.hb.latest("pipe-a")
        assert entry.last_seen == later

    def test_is_alive_false_when_just_expired(self):
        entry = HeartbeatEntry("p", NOW, interval_seconds=30.0)
        check = NOW + timedelta(seconds=31)
        assert not entry.is_alive(now=check)

    def test_seconds_since_calculates_correctly(self):
        entry = HeartbeatEntry("p", NOW, interval_seconds=30.0)
        check = NOW + timedelta(seconds=15)
        assert entry.seconds_since(now=check) == pytest.approx(15.0)

    def test_str_shows_alive(self):
        entry = HeartbeatEntry("pipe-x", NOW, interval_seconds=60.0)
        result = entry.is_alive(now=NOW + timedelta(seconds=5))
        assert result
        s = str(entry)
        assert "pipe-x" in s
