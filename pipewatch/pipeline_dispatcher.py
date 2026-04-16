from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class DispatchRule:
    name: str
    condition: Callable[[PipelineSnapshot], bool]
    target: str

    def matches(self, snapshot: PipelineSnapshot) -> bool:
        try:
            return self.condition(snapshot)
        except Exception:
            return False


@dataclass
class DispatchResult:
    pipeline_id: str
    target: Optional[str]
    rule_name: Optional[str]

    def __str__(self) -> str:
        if self.target:
            return f"{self.pipeline_id} -> {self.target} (rule: {self.rule_name})"
        return f"{self.pipeline_id} -> unmatched"


@dataclass
class DispatcherResult:
    results: List[DispatchResult] = field(default_factory=list)

    def matched(self) -> List[DispatchResult]:
        return [r for r in self.results if r.target is not None]

    def unmatched(self) -> List[DispatchResult]:
        return [r for r in self.results if r.target is None]

    def targets(self) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for r in self.matched():
            out.setdefault(r.target, []).append(r.pipeline_id)
        return out


def dispatch_snapshots(
    snapshots: List[PipelineSnapshot],
    rules: List[DispatchRule],
    default_target: Optional[str] = None,
) -> DispatcherResult:
    results = []
    for snapshot in snapshots:
        matched_rule = None
        for rule in rules:
            if rule.matches(snapshot):
                matched_rule = rule
                break
        if matched_rule:
            results.append(DispatchResult(snapshot.pipeline_id, matched_rule.target, matched_rule.name))
        else:
            results.append(DispatchResult(snapshot.pipeline_id, default_target, None))
    return DispatcherResult(results=results)
