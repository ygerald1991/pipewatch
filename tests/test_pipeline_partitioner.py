import pytest
from unittest.mock import MagicMock
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus
from pipewatch.pipeline_partitioner import (
    PartitionResult,
    partition_by_status,
    partition_by_tag,
    partition_by_error_band,
)


def make_snapshot(pipeline_id="pipe-1", error_rate=0.0, throughput=100.0, status=PipelineStatus.HEALTHY):
    snap = MagicMock(spec=PipelineSnapshot)
    snap.pipeline_id = pipeline_id
    snap.error_rate = error_rate
    snap.throughput = throughput
    snap.status = status
    return snap


class TestPartitionByStatus:
    def test_empty_returns_empty(self):
        assert partition_by_status([]) == {}

    def test_single_snapshot_creates_one_partition(self):
        snap = make_snapshot(status=PipelineStatus.HEALTHY)
        result = partition_by_status([snap])
        assert "healthy" in result
        assert result["healthy"].size == 1

    def test_multiple_statuses_create_separate_partitions(self):
        snaps = [
            make_snapshot("a", status=PipelineStatus.HEALTHY),
            make_snapshot("b", status=PipelineStatus.WARNING),
            make_snapshot("c", status=PipelineStatus.HEALTHY),
        ]
        result = partition_by_status(snaps)
        assert result["healthy"].size == 2
        assert result["warning"].size == 1

    def test_none_status_maps_to_unknown(self):
        snap = make_snapshot()
        snap.status = None
        result = partition_by_status([snap])
        assert "unknown" in result


class TestPartitionByErrorBand:
    def test_empty_returns_empty(self):
        assert partition_by_error_band([]) == {}

    def test_low_error_rate_is_healthy(self):
        snap = make_snapshot(error_rate=0.02)
        result = partition_by_error_band([snap])
        assert "healthy" in result
        assert result["healthy"].size == 1

    def test_mid_error_rate_is_warning(self):
        snap = make_snapshot(error_rate=0.10)
        result = partition_by_error_band([snap])
        assert "warning" in result

    def test_high_error_rate_is_critical(self):
        snap = make_snapshot(error_rate=0.20)
        result = partition_by_error_band([snap])
        assert "critical" in result

    def test_none_error_rate_is_unknown(self):
        snap = make_snapshot()
        snap.error_rate = None
        result = partition_by_error_band([snap])
        assert "unknown" in result

    def test_empty_bands_excluded(self):
        snap = make_snapshot(error_rate=0.01)
        result = partition_by_error_band([snap])
        assert "critical" not in result
        assert "warning" not in result


class TestPartitionResult:
    def test_avg_error_rate_none_when_empty(self):
        p = PartitionResult(key="test")
        assert p.avg_error_rate is None

    def test_avg_error_rate_computed(self):
        snaps = [make_snapshot(error_rate=0.1), make_snapshot(error_rate=0.3)]
        p = PartitionResult(key="test", snapshots=snaps)
        assert p.avg_error_rate == pytest.approx(0.2)

    def test_str_representation(self):
        p = PartitionResult(key="healthy")
        assert "healthy" in str(p)
