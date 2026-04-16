from typing import List
from pipewatch.pipeline_deduplicator import DeduplicationResult


def _format_row(result: DeduplicationResult) -> str:
    status = "clean" if result.duplicates_removed == 0 else "deduped"
    return (
        f"  {result.pipeline_id:<30} "
        f"original={result.original_count:<6} "
        f"unique={result.deduplicated_count:<6} "
        f"removed={result.duplicates_removed:<6} "
        f"[{status}]"
    )


def render_deduplication_table(results: List[DeduplicationResult]) -> str:
    if not results:
        return "No deduplication data available."

    lines = [
        "Pipeline Deduplication Report",
        "=" * 70,
        f"  {'Pipeline':<30} {'Original':<13} {'Unique':<13} {'Removed':<13} Status",
        "-" * 70,
    ]
    for r in sorted(results, key=lambda x: x.pipeline_id):
        lines.append(_format_row(r))
    lines.append("-" * 70)
    total_removed = sum(r.duplicates_removed for r in results)
    lines.append(f"  Total duplicates removed: {total_removed}")
    return "\n".join(lines)


def duplicates_summary(results: List[DeduplicationResult]) -> str:
    affected = [r for r in results if r.duplicates_removed > 0]
    if not affected:
        return "All pipelines are duplicate-free."
    parts = [f"{r.pipeline_id} ({r.duplicates_removed} removed)" for r in affected]
    return "Pipelines with duplicates: " + ", ".join(parts)
