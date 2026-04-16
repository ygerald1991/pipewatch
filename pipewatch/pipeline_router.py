from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.metrics import PipelineStatus


@dataclass
class RouteRule:
    tag: Optional[str] = None
    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    destination: str = "default"

    def matches(self, snapshot: PipelineSnapshot) -> bool:
        if self.tag is not None:
            if self.tag not in (snapshot.tags or []):
                return False
        if self.min_error_rate is not None:
            if snapshot.error_rate is None or snapshot.error_rate < self.min_error_rate:
                return False
        if self.max_error_rate is not None:
            if snapshot.error_rate is None or snapshot.error_rate > self.max_error_rate:
                return False
        return True


@dataclass
class RouteResult:
    pipeline_id: str
    destination: str
    matched_rule: Optional[RouteRule]

    def __str__(self) -> str:
        return f"{self.pipeline_id} -> {self.destination}"


@dataclass
class RouterResult:
    routes: List[RouteResult] = field(default_factory=list)

    def for_destination(self, destination: str) -> List[RouteResult]:
        return [r for r in self.routes if r.destination == destination]

    def destinations(self) -> List[str]:
        return list({r.destination for r in self.routes})


def route_snapshots(
    snapshots: List[PipelineSnapshot],
    rules: List[RouteRule],
    default_destination: str = "default",
) -> RouterResult:
    results: List[RouteResult] = []
    for snapshot in snapshots:
        matched: Optional[RouteRule] = None
        for rule in rules:
            if rule.matches(snapshot):
                matched = rule
                break
        destination = matched.destination if matched else default_destination
        results.append(RouteResult(
            pipeline_id=snapshot.pipeline_id,
            destination=destination,
            matched_rule=matched,
        ))
    return RouterResult(routes=results)
