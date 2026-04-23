import pytest
from datetime import datetime
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_resolver import (
    resolve_snapshots,
    ResolverResult,
    ResolveEntry,
)
from pipewatch.resolver_session import ResolverSession


def make_snapshot(pipeline_id: str) -> PipelineSnapshot:
    return PipelineSnapshot(
        pipeline_id=pipeline_id,
        captured_at=datetime(2024, 1, 1, 12, 0, 0),
        metrics=[],
        status=None,
    )


class TestResolveSnapshots:
    def test_empty_returns_empty(self):
        result = resolve_snapshots([])
        assert isinstance(result, ResolverResult)
        assert result.entries == []

    def test_no_alias_map_leaves_ids_unchanged(self):
        snap = make_snapshot("pipe-a")
        result = resolve_snapshots([snap])
        assert len(result.entries) == 1
        entry = result.entries[0]
        assert entry.pipeline_id == "pipe-a"
        assert entry.resolved_id == "pipe-a"
        assert not entry.was_remapped
        assert entry.alias is None

    def test_alias_map_remaps_matching_pipeline(self):
        snap = make_snapshot("pipe-a")
        result = resolve_snapshots([snap], alias_map={"pipe-a": "production-pipeline"})
        entry = result.entries[0]
        assert entry.resolved_id == "production-pipeline"
        assert entry.alias == "production-pipeline"
        assert entry.was_remapped is True

    def test_non_matching_pipeline_unchanged(self):
        snap = make_snapshot("pipe-b")
        result = resolve_snapshots([snap], alias_map={"pipe-a": "production-pipeline"})
        entry = result.entries[0]
        assert entry.resolved_id == "pipe-b"
        assert not entry.was_remapped

    def test_total_remapped_counts_correctly(self):
        snaps = [make_snapshot("a"), make_snapshot("b"), make_snapshot("c")]
        result = resolve_snapshots(snaps, alias_map={"a": "alpha", "c": "gamma"})
        assert result.total_remapped == 2
        assert result.total_unchanged == 1

    def test_pipeline_ids_returns_resolved_ids(self):
        snaps = [make_snapshot("x"), make_snapshot("y")]
        result = resolve_snapshots(snaps, alias_map={"x": "x-prod"})
        assert "x-prod" in result.pipeline_ids
        assert "y" in result.pipeline_ids

    def test_str_representation_includes_remapped_flag(self):
        entry = ResolveEntry(
            pipeline_id="old", resolved_id="new", alias="new", was_remapped=True
        )
        assert "remapped" in str(entry)

    def test_str_representation_no_flag_when_unchanged(self):
        entry = ResolveEntry(
            pipeline_id="same", resolved_id="same", alias=None, was_remapped=False
        )
        assert "remapped" not in str(entry)


class TestResolverSession:
    def test_empty_session_returns_none(self):
        session = ResolverSession()
        assert session.run() is None

    def test_pipeline_ids_empty_initially(self):
        session = ResolverSession()
        assert session.pipeline_ids == []

    def test_add_snapshot_registers_pipeline(self):
        session = ResolverSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        assert "pipe-1" in session.pipeline_ids

    def test_set_alias_applied_on_run(self):
        session = ResolverSession()
        session.add_snapshot(make_snapshot("pipe-1"))
        session.set_alias("pipe-1", "main-pipeline")
        result = session.run()
        assert result is not None
        assert result.entries[0].resolved_id == "main-pipeline"

    def test_constructor_alias_map_used(self):
        session = ResolverSession(alias_map={"p": "q"})
        session.add_snapshot(make_snapshot("p"))
        result = session.run()
        assert result is not None
        assert result.total_remapped == 1
