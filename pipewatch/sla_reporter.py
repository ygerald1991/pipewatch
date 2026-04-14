"""Human-readable rendering of SLA evaluation results."""

from __future__ import annotations

from pipewatch.sla_tracker import SLAResult

_COL_WIDTH = 20


def _format_row(result: SLAResult) -> str:
    status = "OK" if result.met else "BREACH"
    er_flag = "OK" if result.error_rate_ok else "FAIL"
    tp_flag = "OK" if result.throughput_ok else "FAIL"
    return (
        f"{result.pipeline_id:<{_COL_WIDTH}}"
        f"{status:<10}"
        f"{result.actual_error_rate:>10.2%}"
        f"  ({er_flag})"
        f"{result.actual_throughput:>12.2f}/s"
        f"  ({tp_flag})"
    )


def render_sla_table(results: list[SLAResult]) -> str:
    """Render a plain-text SLA status table."""
    if not results:
        return "No SLA data available."

    header = (
        f"{'Pipeline':<{_COL_WIDTH}}"
        f"{'Status':<10}"
        f"{'Error Rate':>10}"
        f"{'':>7}"
        f"{'Throughput':>12}"
    )
    separator = "-" * len(header)
    rows = [_format_row(r) for r in results]
    return "\n".join([header, separator] + rows)


def overall_sla_status(results: list[SLAResult]) -> str:
    """Return a one-line summary of the overall SLA health."""
    if not results:
        return "No pipelines evaluated."
    breaches = [r for r in results if not r.met]
    if not breaches:
        return f"All {len(results)} pipeline(s) meeting SLA targets."
    ids = ", ".join(r.pipeline_id for r in breaches)
    return f"{len(breaches)}/{len(results)} pipeline(s) breaching SLA: {ids}"
