import pytest

from pipewatch.pipeline_censor import PipelineCensorer, CensorerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(
    pipeline_id: str = "pipe-1",
    error_rate: float = 0.05,
    throughput: float = 100.0,
) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
        metric_count=1,
    )


class TestPipelineCensorer:
    def setup_method(self):
        self.censorer = PipelineCensorer()

    def test_new_censorer_has_no_rules(self):
        assert self.censorer.censored_fields_for("pipe-1") == []

    def test_censor_registers_fields(self):
        self.censorer.censor("pipe-1", ["error_rate"])
        assert "error_rate" in self.censorer.censored_fields_for("pipe-1")

    def test_censor_returns_entry_with_correct_pipeline_id(self):
        entry = self.censorer.censor("pipe-1", ["throughput"])
        assert entry.pipeline_id == "pipe-1"

    def test_censor_entry_contains_redacted_fields(self):
        entry = self.censorer.censor("pipe-1", ["error_rate", "throughput"])
        assert "error_rate" in entry.redacted_fields
        assert "throughput" in entry.redacted_fields

    def test_apply_empty_snapshots_returns_empty(self):
        result = self.censorer.apply([])
        assert isinstance(result, CensorerResult)
        assert result.entries == []

    def test_apply_no_rules_leaves_values_intact(self):
        snap = make_snapshot(error_rate=0.1, throughput=200.0)
        result = self.censorer.apply([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry is not None
        assert entry.error_rate == pytest.approx(0.1)
        assert entry.throughput == pytest.approx(200.0)
        assert entry.censored_fields == []

    def test_apply_censors_error_rate(self):
        self.censorer.censor("pipe-1", ["error_rate"])
        snap = make_snapshot(error_rate=0.2, throughput=50.0)
        result = self.censorer.apply([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry.error_rate is None
        assert entry.throughput == pytest.approx(50.0)

    def test_apply_censors_throughput(self):
        self.censorer.censor("pipe-1", ["throughput"])
        snap = make_snapshot(error_rate=0.05, throughput=999.0)
        result = self.censorer.apply([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry.throughput is None
        assert entry.error_rate == pytest.approx(0.05)

    def test_apply_preserves_original_values(self):
        self.censorer.censor("pipe-1", ["error_rate"])
        snap = make_snapshot(error_rate=0.3, throughput=77.0)
        result = self.censorer.apply([snap])
        entry = result.for_pipeline("pipe-1")
        assert entry.original_error_rate == pytest.approx(0.3)

    def test_total_censored_counts_pipelines_with_rules(self):
        self.censorer.censor("pipe-1", ["error_rate"])
        snaps = [make_snapshot("pipe-1"), make_snapshot("pipe-2")]
        result = self.censorer.apply(snaps)
        assert result.total_censored() == 1

    def test_uncensor_removes_specific_field(self):
        self.censorer.censor("pipe-1", ["error_rate", "throughput"])
        self.censorer.uncensor("pipe-1", ["error_rate"])
        fields = self.censorer.censored_fields_for("pipe-1")
        assert "error_rate" not in fields
        assert "throughput" in fields

    def test_uncensor_all_removes_pipeline_entry(self):
        self.censorer.censor("pipe-1", ["error_rate"])
        self.censorer.uncensor("pipe-1")
        assert self.censorer.censored_fields_for("pipe-1") == []

    def test_pipeline_ids_returns_all_results(self):
        snaps = [make_snapshot("pipe-a"), make_snapshot("pipe-b")]
        result = self.censorer.apply(snaps)
        assert set(result.pipeline_ids()) == {"pipe-a", "pipe-b"}
