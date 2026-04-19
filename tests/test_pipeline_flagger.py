import pytest
from pipewatch.pipeline_flagger import PipelineFlagger, FlaggerResult
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


def make_snapshot(pipeline_id: str, error_rate=0.0, throughput=10.0) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        metric_count=5,
        error_rate=error_rate,
        throughput=throughput,
        status=PipelineStatus.HEALTHY,
    )


@pytest.fixture
def flagger():
    return PipelineFlagger(error_rate_warning=0.05, error_rate_critical=0.15, min_throughput=1.0)


class TestPipelineFlagger:
    def test_empty_returns_empty(self, flagger):
        result = flagger.flag([])
        assert result.total_flagged == 0
        assert result.pipeline_ids == []

    def test_healthy_pipeline_not_flagged(self, flagger):
        snap = make_snapshot("pipe-1", error_rate=0.01, throughput=5.0)
        result = flagger.flag([snap])
        assert result.total_flagged == 0

    def test_warning_on_moderate_error_rate(self, flagger):
        snap = make_snapshot("pipe-2", error_rate=0.08)
        result = flagger.flag([snap])
        assert result.total_flagged == 1
        entry = result.for_pipeline("pipe-2")
        assert entry is not None
        assert entry.severity == "warning"

    def test_critical_on_high_error_rate(self, flagger):
        snap = make_snapshot("pipe-3", error_rate=0.20)
        result = flagger.flag([snap])
        entry = result.for_pipeline("pipe-3")
        assert entry is not None
        assert entry.severity == "critical"

    def test_warning_on_low_throughput(self, flagger):
        snap = make_snapshot("pipe-4", error_rate=0.0, throughput=0.5)
        result = flagger.flag([snap])
        entry = result.for_pipeline("pipe-4")
        assert entry is not None
        assert entry.severity == "warning"

    def test_critical_takes_priority_over_low_throughput(self, flagger):
        snap = make_snapshot("pipe-5", error_rate=0.20, throughput=0.1)
        result = flagger.flag([snap])
        entry = result.for_pipeline("pipe-5")
        assert entry.severity == "critical"

    def test_multiple_pipelines_flagged_correctly(self, flagger):
        snaps = [
            make_snapshot("a", error_rate=0.01, throughput=5.0),
            make_snapshot("b", error_rate=0.10, throughput=5.0),
            make_snapshot("c", error_rate=0.25, throughput=5.0),
        ]
        result = flagger.flag(snaps)
        assert result.total_flagged == 2
        assert "a" not in result.pipeline_ids
        assert "b" in result.pipeline_ids
        assert "c" in result.pipeline_ids

    def test_by_severity_filters_correctly(self, flagger):
        snaps = [
            make_snapshot("x", error_rate=0.08),
            make_snapshot("y", error_rate=0.20),
        ]
        result = flagger.flag(snaps)
        warnings = result.by_severity("warning")
        criticals = result.by_severity("critical")
        assert len(warnings) == 1
        assert len(criticals) == 1

    def test_str_representation(self, flagger):
        snap = make_snapshot("pipe-z", error_rate=0.20)
        result = flagger.flag([snap])
        entry = result.for_pipeline("pipe-z")
        assert "CRITICAL" in str(entry)
        assert "pipe-z" in str(entry)
