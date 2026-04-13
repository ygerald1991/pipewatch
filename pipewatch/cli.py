"""CLI entry point for pipewatch — displays pipeline health reports."""

import argparse
import json
import sys
from datetime import datetime

from pipewatch.metrics import PipelineMetric
from pipewatch.alerts import AlertSeverity, AlertRule
from pipewatch.alert_manager import AlertManager
from pipewatch.reporter import generate_report


DEFAULT_ERROR_THRESHOLD = 0.05
DEFAULT_WARN_THRESHOLD = 0.02


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and report on ETL pipeline health metrics.",
    )
    parser.add_argument("--pipeline-id", required=True, help="Pipeline identifier")
    parser.add_argument("--processed", type=int, required=True, help="Records processed")
    parser.add_argument("--failed", type=int, default=0, help="Records failed")
    parser.add_argument("--duration", type=float, default=1.0, help="Duration in seconds")
    parser.add_argument(
        "--error-threshold", type=float, default=DEFAULT_ERROR_THRESHOLD,
        help="Error rate threshold for CRITICAL status (default: 0.05)",
    )
    parser.add_argument(
        "--warn-threshold", type=float, default=DEFAULT_WARN_THRESHOLD,
        help="Error rate threshold for WARNING status (default: 0.02)",
    )
    parser.add_argument(
        "--format", choices=["text", "json"], default="text",
        help="Output format (default: text)",
    )
    return parser


def run(args=None) -> int:
    parser = build_arg_parser()
    parsed = parser.parse_args(args)

    metric = PipelineMetric(
        pipeline_id=parsed.pipeline_id,
        timestamp=datetime.utcnow(),
        records_processed=parsed.processed,
        records_failed=parsed.failed,
        duration_seconds=parsed.duration,
    )

    manager = AlertManager()
    manager.add_rule(
        AlertRule(
            name="HighErrorRate",
            severity=AlertSeverity.CRITICAL,
            condition=lambda m: (m.records_failed / m.records_processed) >= parsed.error_threshold
            if m.records_processed > 0 else False,
            message_template="Error rate exceeded {threshold:.0%} threshold".format(
                threshold=parsed.error_threshold
            ),
        )
    )

    active_alerts = manager.check(metric)
    report = generate_report(
        pipeline_id=parsed.pipeline_id,
        metric=metric,
        active_alerts=active_alerts,
        error_threshold=parsed.error_threshold,
        warn_threshold=parsed.warn_threshold,
    )

    if parsed.format == "json":
        output = {
            "pipeline_id": report.pipeline_id,
            "generated_at": report.generated_at.isoformat(),
            "status": report.status.value,
            "error_rate": round(report.error_rate, 6),
            "throughput": round(report.throughput, 4),
            "total_processed": report.total_processed,
            "total_failed": report.total_failed,
            "active_alerts": [str(a) for a in report.active_alerts],
        }
        print(json.dumps(output, indent=2))
    else:
        print(report.summary())

    return 1 if active_alerts else 0


if __name__ == "__main__":
    sys.exit(run())
