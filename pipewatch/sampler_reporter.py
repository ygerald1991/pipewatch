from __future__ import annotations
from typing import List

from pipewatch.pipeline_sampler import SampleResult


def _format_row(result: SampleResult) -> str:
    coverage_pct = f"{result.coverage * 100:.1f}%"
    return (
        f"  {result.pipeline_id:<30} "
        f"sampled={result.sample_size:<5} "
        f"total={result.total:<5} "
        f"coverage={coverage_pct}"
    )


def render_sample_table(results: List[SampleResult]) -> str:
    if not results:
        return "No sample results available."

    header = (
        f"  {'Pipeline':<30} {'Sampled':<13} {'Total':<12} Coverage"
    )
    separator = "  " + "-" * 65
    rows = [_format_row(r) for r in results]
    return "\n".join([header, separator] + rows)


def coverage_summary(results: List[SampleResult]) -> str:
    if not results:
        return "No pipelines sampled."
    avg_coverage = sum(r.coverage for r in results) / len(results)
    low = [r.pipeline_id for r in results if r.coverage < 0.5]
    lines = [f"Average coverage: {avg_coverage * 100:.1f}%"]
    if low:
        lines.append(f"Low coverage pipelines (<50%): {', '.join(low)}")
    return "\n".join(lines)
