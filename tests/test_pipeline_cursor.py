import pytest
from pipewatch.pipeline_cursor import PipelineCursor, CursorEntry, CursorResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str = "pipe-1") -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=0.0,
        throughput=100.0,
        status=PipelineStatus.HEALTHY,
        metric_count=5,
    )


class TestPipelineCursor:
    def setup_method(self):
        self.cursor = PipelineCursor()

    def test_new_cursor_has_no_pipeline_ids(self):
        result = self.cursor.run()
        assert result.pipeline_ids == []

    def test_register_adds_pipeline(self):
        snaps = [make_snapshot() for _ in range(3)]
        self.cursor.register("pipe-1", snaps)
        result = self.cursor.run()
        assert "pipe-1" in result.pipeline_ids

    def test_initial_position_is_zero(self):
        snaps = [make_snapshot() for _ in range(5)]
        self.cursor.register("pipe-1", snaps)
        result = self.cursor.run()
        entry = result.for_pipeline("pipe-1")
        assert entry is not None
        assert entry.position == 0

    def test_advance_increments_position(self):
        snaps = [make_snapshot() for _ in range(5)]
        self.cursor.register("pipe-1", snaps)
        entry = self.cursor.advance("pipe-1")
        assert entry.position == 1

    def test_advance_multiple_steps(self):
        snaps = [make_snapshot() for _ in range(10)]
        self.cursor.register("pipe-1", snaps)
        entry = self.cursor.advance("pipe-1", steps=4)
        assert entry.position == 4

    def test_advance_clamps_at_end(self):
        snaps = [make_snapshot() for _ in range(3)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=100)
        result = self.cursor.run()
        entry = result.for_pipeline("pipe-1")
        assert entry.position == 2

    def test_rewind_decrements_position(self):
        snaps = [make_snapshot() for _ in range(5)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=3)
        entry = self.cursor.rewind("pipe-1", steps=2)
        assert entry.position == 1

    def test_rewind_clamps_at_zero(self):
        snaps = [make_snapshot() for _ in range(5)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=2)
        entry = self.cursor.rewind("pipe-1", steps=100)
        assert entry.position == 0

    def test_reset_returns_to_zero(self):
        snaps = [make_snapshot() for _ in range(5)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=3)
        self.cursor.reset("pipe-1")
        result = self.cursor.run()
        assert result.for_pipeline("pipe-1").position == 0

    def test_current_snapshot_returns_correct_snapshot(self):
        snaps = [make_snapshot(f"pipe-{i}") for i in range(5)]
        # use distinct snapshots by overriding pipeline_id for identification
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=2)
        snap = self.cursor.current_snapshot("pipe-1")
        assert snap is snaps[2]

    def test_has_next_false_at_end(self):
        snaps = [make_snapshot() for _ in range(2)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=1)
        result = self.cursor.run()
        entry = result.for_pipeline("pipe-1")
        assert not entry.has_next

    def test_progress_pct_correct(self):
        snaps = [make_snapshot() for _ in range(4)]
        self.cursor.register("pipe-1", snaps)
        self.cursor.advance("pipe-1", steps=2)
        result = self.cursor.run()
        entry = result.for_pipeline("pipe-1")
        assert entry.progress_pct == 50.0

    def test_advance_unknown_pipeline_returns_none(self):
        result = self.cursor.advance("nonexistent")
        assert result is None

    def test_at_end_lists_finished_pipelines(self):
        snaps_a = [make_snapshot() for _ in range(2)]
        snaps_b = [make_snapshot() for _ in range(3)]
        self.cursor.register("pipe-a", snaps_a)
        self.cursor.register("pipe-b", snaps_b)
        self.cursor.advance("pipe-a", steps=1)  # now at end
        result = self.cursor.run()
        at_end_ids = [e.pipeline_id for e in result.at_end()]
        assert "pipe-a" in at_end_ids
        assert "pipe-b" not in at_end_ids
