from dataclasses import dataclass, field
from typing import Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ValidationRule:
    name: str
    description: str
    min_error_rate: Optional[float] = None
    max_error_rate: Optional[float] = None
    min_throughput: Optional[float] = None
    max_throughput: Optional[float] = None

    def check(self, snapshot: PipelineSnapshot) -> Optional[str]:
        er = snapshot.error_rate
        tp = snapshot.throughput
        if self.min_error_rate is not None and er is not None and er < self.min_error_rate:
            return f"{self.name}: error_rate {er:.3f} below min {self.min_error_rate}"
        if self.max_error_rate is not None and er is not None and er > self.max_error_rate:
            return f"{self.name}: error_rate {er:.3f} exceeds max {self.max_error_rate}"
        if self.min_throughput is not None and tp is not None and tp < self.min_throughput:
            return f"{self.name}: throughput {tp:.1f} below min {self.min_throughput}"
        if self.max_throughput is not None and tp is not None and tp > self.max_throughput:
            return f"{self.name}: throughput {tp:.1f} exceeds max {self.max_throughput}"
        return None


@dataclass
class ValidationResult:
    pipeline_id: str
    violations: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.pipeline_id} ({len(self.violations)} violation(s))"


def validate_snapshot(
    snapshot: PipelineSnapshot,
    rules: list,
) -> ValidationResult:
    violations = []
    for rule in rules:
        msg = rule.check(snapshot)
        if msg:
            violations.append(msg)
    return ValidationResult(pipeline_id=snapshot.pipeline_id, violations=violations)


def validate_snapshots(
    snapshots: list,
    rules: list,
) -> list:
    return [validate_snapshot(s, rules) for s in snapshots]
