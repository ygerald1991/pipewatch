import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_sampler import sample_snapshots, SampleResult
from pipewatch.sampler_session import SamplerSession
from pipewatch.sampler_reporter import render_sample_table, coverage_summary


def make_snapshot(pipeline_id: str, idx: int = 0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        status=PipelineStatus.HEALTHY,
        error_rate=0.01,
        throughput=100.0,
        metric_count=5,
        timestamp=1_000_000.0 + idx,
    )


class TestSampleSnapshots:
    def test_empty_returns_empty(self):
        assert sample_snapshots([], n=5) == []

    def test_single_pipeline_sampled(self):
        snaps = [make_snapshot("pipe-a", i) for i in range(10)]
        results = sample_snapshots(snaps, n=3, seed=42)
        assert len(results) == 1
        assert results[0].pipeline_id == "pipe-a"
        assert results[0].sample_size == 3
        assert results[0].total == 10

    def test_sample_size_capped_at_total(self):
        snaps = [make_snapshot("pipe-b", i) for i in range(4)]
        results = sample_snapshots(snaps, n=100, seed=0)
        assert results[0].sample_size == 4

    def test_multiple_pipelines_each_sampled(self):
        snaps = (
            [make_snapshot("alpha", i) for i in range(6)]
            + [make_snapshot("beta", i) for i in range(8)]
        )
        results = sample_snapshots(snaps, n=3, seed=7)
        ids = [r.pipeline_id for r in results]
        assert "alpha" in ids
        assert "beta" in ids

    def test_coverage_is_correct(self):
        snaps = [make_snapshot("pipe-c", i) for i in range(10)]
        results = sample_snapshots(snaps, n=5, seed=1)
        assert abs(results[0].coverage - 0.5) < 1e-9

    def test_seed_produces_deterministic_results(self):
        snaps = [make_snapshot("pipe-d", i) for i in range(20)]
        r1 = sample_snapshots(snaps, n=5, seed=99)
        r2 = sample_snapshots(snaps, n=5, seed=99)
        assert [s.timestamp for s in r1[0].selected] == [
            s.timestamp for s in r2[0].selected
        ]


class TestSamplerSession:
    def test_empty_session_returns_empty(self):
        session = SamplerSession(n=5)
        assert session.run() == []

    def test_add_snapshot_registers_pipeline(self):
        session = SamplerSession(n=5)
        session.add_snapshot(make_snapshot("p1"))
        assert "p1" in session.pipeline_ids

    def test_total_snapshots_counts_all(self):
        session = SamplerSession(n=5)
        for i in range(7):
            session.add_snapshot(make_snapshot("px", i))
        assert session.total_snapshots() == 7

    def test_run_returns_sample_results(self):
        session = SamplerSession(n=2, seed=0)
        for i in range(5):
            session.add_snapshot(make_snapshot("q1", i))
        results = session.run()
        assert len(results) == 1
        assert results[0].sample_size == 2


class TestSamplerReporter:
    def test_empty_returns_no_data_message(self):
        assert "No sample" in render_sample_table([])

    def test_output_contains_pipeline_id(self):
        snaps = [make_snapshot("my-pipe", i) for i in range(5)]
        results = sample_snapshots(snaps, n=3, seed=0)
        out = render_sample_table(results)
        assert "my-pipe" in out

    def test_coverage_summary_shows_average(self):
        snaps = [make_snapshot("z1", i) for i in range(10)]
        results = sample_snapshots(snaps, n=5, seed=0)
        summary = coverage_summary(results)
        assert "Average coverage" in summary

    def test_coverage_summary_empty(self):
        assert "No pipelines" in coverage_summary([])
