import pytest
from pipewatch.watcher_session import WatcherSession
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.01,
        throughput=50.0,
        metric_count=2,
    )


class TestWatcherSession:
    def setup_method(self):
        self.session = WatcherSession()

    def test_empty_session_has_no_pipeline_ids(self):
        assert self.session.pipeline_ids == []

    def test_watch_and_add_snapshot(self):
        self.session.watch("pipe-a")
        self.session.add_snapshot(make_snapshot("pipe-a"))
        assert "pipe-a" in self.session.pipeline_ids

    def test_active_snapshots_excludes_unwatched(self):
        self.session.watch("pipe-a")
        self.session.add_snapshot(make_snapshot("pipe-a"))
        self.session.add_snapshot(make_snapshot("pipe-b"))  # not watched
        active = self.session.active_snapshots()
        assert all(s.pipeline_id == "pipe-a" for s in active)

    def test_unwatch_removes_snapshots(self):
        self.session.watch("pipe-a")
        self.session.add_snapshot(make_snapshot("pipe-a"))
        self.session.unwatch("pipe-a")
        assert "pipe-a" not in self.session.pipeline_ids

    def test_run_returns_watcher_result(self):
        self.session.watch("pipe-x")
        result = self.session.run()
        assert "pipe-x" in result.pipeline_ids

    def test_run_reflects_paused_pipeline(self):
        self.session.watch("pipe-a")
        self.session._watcher.pause("pipe-a")
        result = self.session.run()
        assert result.total_paused == 1
        assert result.total_watching == 0
