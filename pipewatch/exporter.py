"""Export pipeline metrics and reports to various output formats."""

from __future__ import annotations

import csv
import json
import io
from typing import List

from pipewatch.reporter import PipelineReport


def export_json(reports: List[PipelineReport], indent: int = 2) -> str:
    """Serialize a list of PipelineReport objects to a JSON string."""
    data = [
        {
            "pipeline_id": r.pipeline_id,
            "status": r.status.value,
            "error_rate": r.error_rate,
            "throughput": r.throughput,
            "total_records": r.total_records,
            "total_failures": r.total_failures,
            "window_seconds": r.window_seconds,
        }
        for r in reports
    ]
    return json.dumps(data, indent=indent)


def export_csv(reports: List[PipelineReport]) -> str:
    """Serialize a list of PipelineReport objects to a CSV string."""
    fieldnames = [
        "pipeline_id",
        "status",
        "error_rate",
        "throughput",
        "total_records",
        "total_failures",
        "window_seconds",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in reports:
        writer.writerow(
            {
                "pipeline_id": r.pipeline_id,
                "status": r.status.value,
                "error_rate": round(r.error_rate, 6),
                "throughput": round(r.throughput, 6),
                "total_records": r.total_records,
                "total_failures": r.total_failures,
                "window_seconds": r.window_seconds,
            }
        )
    return output.getvalue()


def export_text(reports: List[PipelineReport]) -> str:
    """Serialize a list of PipelineReport objects to a human-readable text block."""
    lines = []
    for r in reports:
        lines.append(f"Pipeline : {r.pipeline_id}")
        lines.append(f"  Status      : {r.status.value}")
        lines.append(f"  Error Rate  : {r.error_rate:.2%}")
        lines.append(f"  Throughput  : {r.throughput:.2f} rec/s")
        lines.append(f"  Records     : {r.total_records}")
        lines.append(f"  Failures    : {r.total_failures}")
        lines.append(f"  Window      : {r.window_seconds}s")
        lines.append("")
    return "\n".join(lines).rstrip()
