from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class TaggingRule:
    tag: str
    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    min_throughput: Optional[float] = None
    max_throughput: Optional[float] = None

    def matches(self, snapshot: PipelineSnapshot) -> bool:
        er = snapshot.error_rate
        tp = snapshot.throughput
        if self.min_error_rate is not None and (er is None or er < self.min_error_rate):
            return False
        if self.max_error_rate is not None and (er is None or er > self.max_error_rate):
            return False
        if self.min_throughput is not None and (tp is None or tp < self.min_throughput):
            return False
        if self.max_throughput is not None and (tp is None or tp > self.max_throughput):
            return False
        return True


@dataclass
class TaggingResult:
    pipeline_id: str
    tags: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tag_str = ", ".join(self.tags) if self.tags else "(none)"
        return f"{self.pipeline_id}: [{tag_str}]"


def apply_tagging_rules(
    snapshots: List[PipelineSnapshot],
    rules: List[TaggingRule],
) -> List[TaggingResult]:
    results = []
    for snap in snapshots:
        matched = [r.tag for r in rules if r.matches(snap)]
        results.append(TaggingResult(pipeline_id=snap.pipeline_id, tags=matched))
    return results
