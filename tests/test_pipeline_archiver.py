"""Tests for pipeline_archiver, archive_session, and archive_reporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric
from pipewatch.pipeline_archiver import ArchiveEntry, ArchiveStore
from pipewatch.archive_session import ArchiveSession
from pipewatch.archive_reporter import render_archive_table, archive_summary


def make_metric(pipeline_id: str, processed: int = 100, failed: int = 5) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        records_processed=processed,
        records_failed=failed,
        duration_seconds=10.0,
    )


def make_collector(pipeline_id: str, n: int = 3) -> MetricsCollector:
    c = MetricsCollector()
    for _ in range(n):
        c.record(make_metric(pipeline_id))
    return c


class TestArchiveStore:
    def test_empty_store_has_no_pipelines(self):
        store = ArchiveStore()
        assert store.pipeline_ids() == []

    def test_archive_adds_entry(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-a")
        snapshot = capture_snapshot("pipe-a", collector)
        entry = store.archive(snapshot)
        assert entry.pipeline_id == "pipe-a"
        assert len(store.entries_for("pipe-a")) == 1

    def test_latest_for_returns_most_recent(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-b")
        snap = capture_snapshot("pipe-b", collector)
        store.archive(snap)
        store.archive(snap)
        assert store.latest_for("pipe-b") is not None
        assert store.total_entries() == 2

    def test_max_entries_rotation(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore(max_entries_per_pipeline=3)
        collector = make_collector("pipe-c", n=5)
        snap = capture_snapshot("pipe-c", collector)
        for _ in range(5):
            store.archive(snap)
        assert len(store.entries_for("pipe-c")) == 3

    def test_prune_reduces_entries(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-d")
        snap = capture_snapshot("pipe-d", collector)
        for _ in range(5):
            store.archive(snap)
        removed = store.prune("pipe-d", keep=2)
        assert removed == 3
        assert len(store.entries_for("pipe-d")) == 2

    def test_export_json_is_valid(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-e")
        snap = capture_snapshot("pipe-e", collector)
        store.archive(snap)
        raw = store.export_json("pipe-e")
        data = json.loads(raw)
        assert isinstance(data, list)
        assert data[0]["pipeline_id"] == "pipe-e"


class TestArchiveSession:
    def test_run_returns_entries_for_valid_pipelines(self):
        session = ArchiveSession()
        session.register("pipe-1", make_collector("pipe-1"))
        results = session.run()
        assert "pipe-1" in results

    def test_run_skips_empty_collectors(self):
        session = ArchiveSession()
        session.register("pipe-empty", MetricsCollector())
        results = session.run()
        assert "pipe-empty" not in results

    def test_pipeline_ids_reflects_registered(self):
        session = ArchiveSession()
        session.register("a", make_collector("a"))
        session.register("b", make_collector("b"))
        assert set(session.pipeline_ids()) == {"a", "b"}


class TestArchiveReporter:
    def test_render_archive_table_no_entries(self):
        store = ArchiveStore()
        out = render_archive_table(store, "missing")
        assert "No archive entries" in out

    def test_render_archive_table_contains_pipeline_id(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-r")
        snap = capture_snapshot("pipe-r", collector)
        store.archive(snap)
        out = render_archive_table(store, "pipe-r")
        assert "pipe-r" in out

    def test_archive_summary_empty(self):
        store = ArchiveStore()
        assert "empty" in archive_summary(store).lower()

    def test_archive_summary_shows_counts(self):
        from pipewatch.snapshot import capture_snapshot
        store = ArchiveStore()
        collector = make_collector("pipe-s")
        snap = capture_snapshot("pipe-s", collector)
        store.archive(snap)
        store.archive(snap)
        out = archive_summary(store)
        assert "pipe-s" in out
        assert "2" in out
