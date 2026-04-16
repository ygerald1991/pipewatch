from __future__ import annotations
from collections import Counter
from pipewatch.pipeline_classifier import ClassificationResult

_CATEGORY_ORDER = ["critical", "degraded", "idle", "healthy", "unknown"]


def _format_row(result: ClassificationResult) -> str:
    er = f"{result.error_rate:.2%}" if result.error_rate is not None else "n/a"
    tp = f"{result.throughput:.1f}" if result.throughput is not None else "n/a"
    return (
        f"  {result.pipeline_id:<24} {result.category:<10} "
        f"{er:>8}  {tp:>10}  {result.reason}"
    )


def render_classification_table(results: list[ClassificationResult]) -> str:
    if not results:
        return "No classification data available."
    header = (
        f"  {'Pipeline':<24} {'Category':<10} "
        f"{'ErrRate':>8}  {'Throughput':>10}  Reason"
    )
    separator = "  " + "-" * 74
    ordered = sorted(
        results,
        key=lambda r: (_CATEGORY_ORDER.index(r.category)
                       if r.category in _CATEGORY_ORDER else 99,
                       r.pipeline_id),
    )
    rows = [_format_row(r) for r in ordered]
    return "\n".join([header, separator] + rows)


def category_summary(results: list[ClassificationResult]) -> str:
    if not results:
        return "No results to summarise."
    counts: Counter[str] = Counter(r.category for r in results)
    parts = [f"{cat}: {counts[cat]}" for cat in _CATEGORY_ORDER if counts[cat]]
    return "Classification summary — " + ", ".join(parts)
