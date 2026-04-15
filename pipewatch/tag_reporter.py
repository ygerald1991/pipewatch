"""Renders a summary table of pipelines grouped by tag."""
from __future__ import annotations
from typing import Dict, List

from pipewatch.tag_filter import TagRegistry


def _collect_tag_groups(registry: TagRegistry) -> Dict[str, List[str]]:
    """Build a mapping of tag -> sorted list of pipeline IDs."""
    groups: Dict[str, List[str]] = {}
    for pid in registry.pipeline_ids():
        for tag in registry.tags_for(pid):
            groups.setdefault(tag, []).append(pid)
    for pipelines in groups.values():
        pipelines.sort()
    return groups


def render_tag_table(registry: TagRegistry) -> str:
    """Return a formatted table of tags and their associated pipelines."""
    groups = _collect_tag_groups(registry)
    if not groups:
        return "No tags registered."

    col_tag = max(len(t) for t in groups) + 2
    col_tag = max(col_tag, 10)
    header = f"{'TAG':<{col_tag}}  PIPELINES"
    separator = "-" * len(header)
    lines = [header, separator]
    for tag in sorted(groups):
        pipelines_str = ", ".join(groups[tag])
        lines.append(f"{tag:<{col_tag}}  {pipelines_str}")
    return "\n".join(lines)


def pipelines_for_tag_summary(registry: TagRegistry, tag: str) -> str:
    """Return a one-line summary of pipelines carrying *tag*."""
    pipelines = sorted(
        pid
        for pid in registry.pipeline_ids()
        if tag in registry.tags_for(pid)
    )
    if not pipelines:
        return f"No pipelines tagged '{tag}'."
    return f"Tag '{tag}': " + ", ".join(pipelines)
