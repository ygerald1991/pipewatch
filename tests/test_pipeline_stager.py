import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import P
from pipewatch.pipeline_stager import stage_snapshots, StagerResult


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=1,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


class TestStageSnapshots:
    def test_empty_returns_empty(self):
        result = stage_snapshots([], {})
        assert result.stages == []

    def test_single_snapshot_uses_default_stage(self):
        snap = make_snapshot("pipe-1")
        result = stage_snapshots([snap], {})
        assert len(result.stages) == 1
        assert result.stages[0].stage == "default"

    def test_stage_map_assigns_correct_stage(self):
        snap = make_snapshot("pipe-1")
        result = stage_snapshots([snap], {"pipe-1": "ingest"})
        assert result.stages[0].stage == "ingest"

    def test_pipeline_id_is_set(self):
        snap = make_snapshot("pipe-A")
        result = stage_snapshots([snap], {})
        assert result.stages[0].pipeline_id == "pipe-A"

    def test_snapshot_count_is_correct(self):
        snaps = [make_snapshot("pipe-1") for _ in range(4)]
        result = stage_snapshots(snaps, {"pipe-1": "transform"})
        assert result.stages[0].snapshot_count == 4

    def test_avg_error_rate_computed(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.1),
            make_snapshot("pipe-1", error_rate=0.3),
        ]
        result = stage_snapshots(snaps, {})
        assert abs(result.stages[0].avg_error_rate - 0.2) < 1e-9

    def test_avg_throughput_computed(self):
        snaps = [
            make_snapshot("pipe-1", throughput=200.0),
            make_snapshot("pipe-1", throughput=400.0),
        ]
        result = stage_snapshots(snaps, {})
        assert abs(result.stages[0].avg_throughput - 300.0) < 1e-9

    def test_multiple_pipelines_different_stages(self):
        snaps = [
            make_snapshot("pipe-1"),
            make_snapshot("pipe-2"),
        ]
        stage_map = {"pipe-1": "ingest", "pipe-2": "load"}
        result = stage_snapshots(snaps, stage_map)
        stages = {r.stage for r in result.stages}
        assert stages == {"ingest", "load"}

    def test_pipeline_ids_returns_unique_ids(self):
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = stage_snapshots(snaps, {})
        assert set(result.pipeline_ids()) == {"pipe-1", "pipe-2"}

    def test_for_stage_filters_correctly(self):
        snaps = [
            make_snapshot("pipe-1"),
            make_snapshot("pipe-2"),
        ]
        stage_map = {"pipe-1": "ingest", "pipe-2": "load"}
        result = stage_snapshots(snaps, stage_map)
        ingest = result.for_stage("ingest")
        assert len(ingest) == 1
        assert ingest[0].pipeline_id == "pipe-1"

    def test_results_sorted_by_stage_then_pipeline(self):
        snaps = [make_snapshot("b-pipe"), make_snapshot("a-pipe")]
        result = stage_snapshots(snaps, {})
        ids = [r.pipeline_id for r in result.stages]
        assert ids == sorted(ids)
