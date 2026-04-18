import pytest
from pipewatch.pipeline_watcher import PipelineWatcher, WatchEntry, WatcherResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.0,
        throughput=100.0,
        metric_count=1,
    )


class TestPipelineWatcher:
    def setup_method(self):
        self.watcher = PipelineWatcher()

    def test_new_watcher_has_no_entries(self):
        result = self.watcher.result()
        assert result.pipeline_ids == []

    def test_watch_registers_pipeline(self):
        self.watcher.watch("pipe-a")
        assert self.watcher.is_watching("pipe-a")

    def test_unwatch_removes_pipeline(self):
        self.watcher.watch("pipe-a")
        removed = self.watcher.unwatch("pipe-a")
        assert removed is True
        assert not self.watcher.is_watching("pipe-a")

    def test_unwatch_unknown_returns_false(self):
        assert self.watcher.unwatch("ghost") is False

    def test_pause_disables_watching(self):
        self.watcher.watch("pipe-a")
        self.watcher.pause("pipe-a")
        assert not self.watcher.is_watching("pipe-a")

    def test_resume_re_enables_watching(self):
        self.watcher.watch("pipe-a")
        self.watcher.pause("pipe-a")
        self.watcher.resume("pipe-a")
        assert self.watcher.is_watching("pipe-a")

    def test_pause_unknown_returns_false(self):
        assert self.watcher.pause("ghost") is False

    def test_result_counts_watching_and_paused(self):
        self.watcher.watch("pipe-a")
        self.watcher.watch("pipe-b")
        self.watcher.pause("pipe-b")
        result = self.watcher.result()
        assert result.total_watching == 1
        assert result.total_paused == 1

    def test_filter_snapshots_excludes_unwatched(self):
        self.watcher.watch("pipe-a")
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        filtered = self.watcher.filter_snapshots(snaps)
        assert len(filtered) == 1
        assert filtered[0].pipeline_id == "pipe-a"

    def test_filter_snapshots_excludes_paused(self):
        self.watcher.watch("pipe-a")
        self.watcher.pause("pipe-a")
        snaps = [make_snapshot("pipe-a")]
        filtered = self.watcher.filter_snapshots(snaps)
        assert filtered == []

    def test_notes_stored_on_entry(self):
        entry = self.watcher.watch("pipe-a", notes="critical pipeline")
        assert entry.notes == "critical pipeline"
