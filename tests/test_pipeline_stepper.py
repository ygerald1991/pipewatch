from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.pipeline_stepper import PipelineStepper, StepEntry, StepperResult
from pipewatch.snapshot import PipelineSnapshot


def make_snapshot(pipeline_id: str = "pipe-1") -> PipelineSnapshot:
    snap = MagicMock(spec=PipelineSnapshot)
    snap.pipeline_id = pipeline_id
    return snap


class TestPipelineStepper:
    def setup_method(self):
        self.stepper = PipelineStepper(step_names=["extract", "transform", "load"])

    def test_new_stepper_has_no_pipeline_ids(self):
        assert self.stepper.pipeline_ids == []

    def test_advance_registers_pipeline(self):
        snap = make_snapshot("pipe-a")
        self.stepper.advance(snap)
        assert "pipe-a" in self.stepper.pipeline_ids

    def test_first_step_has_index_zero(self):
        snap = make_snapshot("pipe-a")
        entry = self.stepper.advance(snap)
        assert entry.step_index == 0

    def test_step_name_from_list(self):
        snap = make_snapshot("pipe-a")
        entry = self.stepper.advance(snap)
        assert entry.step_name == "extract"

    def test_second_advance_increments_index(self):
        snap = make_snapshot("pipe-a")
        self.stepper.advance(snap)
        entry2 = self.stepper.advance(make_snapshot("pipe-a"))
        assert entry2.step_index == 1
        assert entry2.step_name == "transform"

    def test_step_beyond_named_list_uses_fallback(self):
        stepper = PipelineStepper(step_names=["only"])
        snap = make_snapshot("pipe-x")
        stepper.advance(snap)
        entry = stepper.advance(make_snapshot("pipe-x"))
        assert entry.step_name == "step_1"

    def test_result_for_unknown_pipeline_returns_none(self):
        assert self.stepper.result_for("unknown") is None

    def test_result_for_known_pipeline(self):
        snap = make_snapshot("pipe-b")
        self.stepper.advance(snap)
        result = self.stepper.result_for("pipe-b")
        assert result is not None
        assert result.pipeline_id == "pipe-b"

    def test_total_steps_matches_advance_count(self):
        snap = make_snapshot("pipe-c")
        for _ in range(3):
            self.stepper.advance(snap)
        result = self.stepper.result_for("pipe-c")
        assert result.total_steps == 3

    def test_current_step_is_last_advanced(self):
        snap = make_snapshot("pipe-d")
        self.stepper.advance(snap)
        entry = self.stepper.advance(make_snapshot("pipe-d"))
        result = self.stepper.result_for("pipe-d")
        assert result.current_step is entry

    def test_current_step_none_for_empty_result(self):
        result = StepperResult(pipeline_id="empty")
        assert result.current_step is None

    def test_all_results_returns_all_pipelines(self):
        self.stepper.advance(make_snapshot("p1"))
        self.stepper.advance(make_snapshot("p2"))
        ids = {r.pipeline_id for r in self.stepper.all_results()}
        assert ids == {"p1", "p2"}

    def test_separate_pipelines_tracked_independently(self):
        self.stepper.advance(make_snapshot("alpha"))
        self.stepper.advance(make_snapshot("alpha"))
        self.stepper.advance(make_snapshot("beta"))
        assert self.stepper.result_for("alpha").total_steps == 2
        assert self.stepper.result_for("beta").total_steps == 1

    def test_str_representation(self):
        result = StepperResult(pipeline_id="pipe-z")
        assert "pipe-z" in str(result)
