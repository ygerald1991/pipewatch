"""Tests for pipewatch.exporter — JSON, CSV, and text export helpers."""

from __future__ import annotations

import csv
import io
import json

import pytest

from pipewatch.metrics import PipelineMetric, PipelineStatus
from pipewatch.reporter import generate_report
from pipewatch.exporter import export_csv, export_json, export_text


def make_metric(
    pipeline_id: str = "pipe-1",
    records: int = 100,
    failures: int = 5,
    duration: float = 60.0,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        total_records=records,
        total_failures=failures,
        window_seconds=duration,
    )


class TestExportJson:
    def test_returns_valid_json(self):
        report = generate_report(make_metric())
        result = export_json([report])
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 1

    def test_json_contains_expected_keys(self):
        report = generate_report(make_metric())
        parsed = json.loads(export_json([report]))
        keys = parsed[0].keys()
        for field in ("pipeline_id", "status", "error_rate", "throughput"):
            assert field in keys

    def test_json_pipeline_id_matches(self):
        report = generate_report(make_metric(pipeline_id="alpha"))
        parsed = json.loads(export_json([report]))
        assert parsed[0]["pipeline_id"] == "alpha"

    def test_json_multiple_reports(self):
        reports = [generate_report(make_metric(pipeline_id=f"p{i}")) for i in range(3)]
        parsed = json.loads(export_json(reports))
        assert len(parsed) == 3

    def test_json_status_is_string(self):
        report = generate_report(make_metric())
        parsed = json.loads(export_json([report]))
        assert isinstance(parsed[0]["status"], str)


class TestExportCsv:
    def _parse(self, text: str):
        return list(csv.DictReader(io.StringIO(text)))

    def test_returns_csv_with_header(self):
        report = generate_report(make_metric())
        result = export_csv([report])
        assert "pipeline_id" in result.splitlines()[0]

    def test_csv_row_count_matches_reports(self):
        reports = [generate_report(make_metric(pipeline_id=f"p{i}")) for i in range(4)]
        rows = self._parse(export_csv(reports))
        assert len(rows) == 4

    def test_csv_pipeline_id_correct(self):
        report = generate_report(make_metric(pipeline_id="beta"))
        rows = self._parse(export_csv([report]))
        assert rows[0]["pipeline_id"] == "beta"

    def test_csv_empty_list(self):
        result = export_csv([])
        rows = self._parse(result)
        assert rows == []


class TestExportText:
    def test_contains_pipeline_id(self):
        report = generate_report(make_metric(pipeline_id="gamma"))
        result = export_text([report])
        assert "gamma" in result

    def test_contains_status(self):
        report = generate_report(make_metric(failures=0))
        result = export_text([report])
        assert report.status.value in result

    def test_multiple_pipelines_all_present(self):
        ids = ["pipe-x", "pipe-y", "pipe-z"]
        reports = [generate_report(make_metric(pipeline_id=pid)) for pid in ids]
        result = export_text(reports)
        for pid in ids:
            assert pid in result

    def test_empty_list_returns_empty_string(self):
        assert export_text([]) == ""
