from __future__ import annotations
from typing import List
from pipewatch.pipeline_tagger import TaggingResult


def _format_row(result: TaggingResult) -> str:
    tag_str = ", ".join(result.tags) if result.tags else "(none)"
    return f"  {result.pipeline_id:<30} {tag_str}"


def render_tagging_table(results: List[TaggingResult]) -> str:
    if not results:
        return "No pipelines to display."
    header = f"  {'PIPELINE':<30} TAGS"
    separator = "  " + "-" * 60
    rows = [header, separator] + [_format_row(r) for r in results]
    return "\n".join(rows)


def tagged_summary(results: List[TaggingResult]) -> str:
    if not results:
        return "No tagging results."
    all_tags: dict[str, list[str]] = {}
    for r in results:
        for tag in r.tags:
            all_tags.setdefault(tag, []).append(r.pipeline_id)
    if not all_tags:
        return "No tags matched any pipeline."
    lines = []
    for tag, pipelines in sorted(all_tags.items()):
        lines.append(f"  {tag}: {', '.join(pipelines)}")
    return "\n".join(lines)
