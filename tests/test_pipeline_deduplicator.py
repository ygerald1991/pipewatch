import pytest
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_deduplicator import (
    deduplicate_snapshots,
    deduplicate_all,
    DeduplicationResult,
)


def make_snapshot(pipeline_id: str, error_rate: float = 0.0, throughput: float = 100.0, metric_count: int = 5) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        error_rate=error_rate,
        throughput=throughput,
        metric_count=metric_count,
        status="healthy",
    )


class TestDeduplicateSnapshots:
    def test_empty_returns_empty_result(self):
        result = deduplicate_snapshots("pipe-1", [])
        assert result.original_count == 0
        assert result.deduplicated_count == 0
        assert result.duplicates_removed == 0
        assert result.representative is None

    def test_single_snapshot_no_duplicates(self):
        snap = make_snapshot("pipe-1")
        result = deduplicate_snapshots("pipe-1", [snap])
        assert result.original_count == 1
        assert result.deduplicated_count == 1
        assert result.duplicates_removed == 0

    def test_identical_snapshots_deduplicated(self):
        snaps = [make_snapshot("pipe-1") for _ in range(4)]
        result = deduplicate_snapshots("pipe-1", snaps)
        assert result.original_count == 4
        assert result.deduplicated_count == 1
        assert result.duplicates_removed == 3

    def test_distinct_snapshots_all_kept(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.1),
            make_snapshot("pipe-1", error_rate=0.2),
            make_snapshot("pipe-1", error_rate=0.3),
        ]
        result = deduplicate_snapshots("pipe-1", snaps)
        assert result.deduplicated_count == 3
        assert result.duplicates_removed == 0

    def test_partial_duplicates(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.1),
            make_snapshot("pipe-1", error_rate=0.1),
            make_snapshot("pipe-1", error_rate=0.2),
        ]
        result = deduplicate_snapshots("pipe-1", snaps)
        assert result.deduplicated_count == 2
        assert result.duplicates_removed == 1

    def test_representative_is_first_unique(self):
        snaps = [
            make_snapshot("pipe-1", error_rate=0.5),
            make_snapshot("pipe-1", error_rate=0.5),
        ]
        result = deduplicate_snapshots("pipe-1", snaps)
        assert result.representative is not None
        assert result.representative.error_rate == 0.5

    def test_pipeline_id_preserved(self):
        snap = make_snapshot("my-pipeline")
        result = deduplicate_snapshots("my-pipeline", [snap])
        assert result.pipeline_id == "my-pipeline"

    def test_str_representation(self):
        snap = make_snapshot("pipe-1")
        result = deduplicate_snapshots("pipe-1", [snap, snap])
        s = str(result)
        assert "pipe-1" in s

    def test_deduplicate_all_multiple_pipelines(self):
        groups = {
            "pipe-a": [make_snapshot("pipe-a"), make_snapshot("pipe-a")],
            "pipe-b": [make_snapshot("pipe-b", error_rate=0.1)],
        }
        results = deduplicate_all(groups)
        assert len(results) == 2
        ids = {r.pipeline_id for r in results}
        assert "pipe-a" in ids
        assert "pipe-b" in ids

    def test_deduplicate_all_empty(self):
        assert deduplicate_all({}) == []
