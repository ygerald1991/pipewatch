import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_bumper import PipelineBumper, BumperResult, BumpEntry
from pipewatch.bumper_reporter import render_bumper_table, bumped_summary


def make_snapshot(pipeline_id: str, error_rate: float = 0.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime.utcnow(),
        metric_count=1,
        error_rate=error_rate,
        throughput=100.0,
        status="healthy",
    )


class TestPipelineBumper:
    def setup_method(self):
        self.bumper = PipelineBumper(default_priority=5)

    def test_new_bumper_default_priority(self):
        assert self.bumper.priority_for("pipe-a") == 5

    def test_bump_increases_priority(self):
        entry = self.bumper.bump("pipe-a", by=2, reason="test")
        assert entry.new_priority == 7
        assert entry.previous_priority == 5

    def test_bump_accumulates(self):
        self.bumper.bump("pipe-a", by=1)
        self.bumper.bump("pipe-a", by=1)
        assert self.bumper.priority_for("pipe-a") == 7

    def test_reset_restores_default(self):
        self.bumper.bump("pipe-a", by=3)
        self.bumper.reset("pipe-a")
        assert self.bumper.priority_for("pipe-a") == 5

    def test_run_no_bumps_when_all_healthy(self):
        snaps = [make_snapshot("pipe-a", 0.0), make_snapshot("pipe-b", 0.05)]
        result = self.bumper.run(snaps, threshold=0.1)
        assert result.total_bumped == 0

    def test_run_bumps_high_error_rate(self):
        snaps = [make_snapshot("pipe-a", 0.15), make_snapshot("pipe-b", 0.02)]
        result = self.bumper.run(snaps, threshold=0.1)
        assert result.total_bumped == 1
        assert result.pipeline_ids == ["pipe-a"]

    def test_run_bumps_at_exact_threshold(self):
        snaps = [make_snapshot("pipe-a", 0.10)]
        result = self.bumper.run(snaps, threshold=0.10)
        assert result.total_bumped == 1

    def test_for_pipeline_returns_entry(self):
        snaps = [make_snapshot("pipe-a", 0.2)]
        result = self.bumper.run(snaps)
        entry = result.for_pipeline("pipe-a")
        assert entry is not None
        assert entry.reason == "high_error_rate"

    def test_for_pipeline_returns_none_when_missing(self):
        result = BumperResult(entries=[])
        assert result.for_pipeline("pipe-x") is None


class TestBumperReporter:
    def test_empty_result_returns_no_pipelines_message(self):
        result = BumperResult(entries=[])
        assert "No pipelines bumped" in render_bumper_table(result)

    def test_none_returns_no_pipelines_message(self):
        assert "No pipelines bumped" in render_bumper_table(None)

    def test_output_contains_pipeline_id(self):
        entry = BumpEntry(pipeline_id="pipe-a", previous_priority=5, new_priority=6, reason="test")
        result = BumperResult(entries=[entry])
        assert "pipe-a" in render_bumper_table(result)

    def test_output_contains_reason(self):
        entry = BumpEntry(pipeline_id="pipe-a", previous_priority=5, new_priority=6, reason="high_error_rate")
        result = BumperResult(entries=[entry])
        assert "high_error_rate" in render_bumper_table(result)

    def test_bumped_summary_lists_pipeline(self):
        entry = BumpEntry(pipeline_id="pipe-a", previous_priority=5, new_priority=6, reason="test")
        result = BumperResult(entries=[entry])
        summary = bumped_summary(result)
        assert "pipe-a" in summary
        assert "1" in summary
