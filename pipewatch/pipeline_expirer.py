"""Pipeline expirer — marks pipelines as expired based on inactivity or TTL policies."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.snapshot import PipelineSnapshot


@dataclass
class ExpireEntry:
    pipeline_id: str
    expired_at: datetime
    reason: str
    ttl_seconds: float

    def __str__(self) -> str:
        return (
            f"ExpireEntry(pipeline={self.pipeline_id}, "
            f"expired_at={self.expired_at.isoformat()}, reason={self.reason})"
        )


@dataclass
class ExpirerResult:
    expired: List[ExpireEntry] = field(default_factory=list)
    active: List[str] = field(default_factory=list)

    @property
    def total_expired(self) -> int:
        return len(self.expired)

    @property
    def total_active(self) -> int:
        return len(self.active)

    @property
    def pipeline_ids(self) -> List[str]:
        return [e.pipeline_id for e in self.expired] + self.active

    def is_expired(self, pipeline_id: str) -> bool:
        return any(e.pipeline_id == pipeline_id for e in self.expired)


class PipelineExpirer:
    """Tracks pipeline TTLs and marks them expired when the TTL elapses."""

    def __init__(self, default_ttl_seconds: float = 3600.0) -> None:
        self._default_ttl = default_ttl_seconds
        self._ttl_overrides: Dict[str, float] = {}
        self._last_seen: Dict[str, datetime] = {}

    def set_ttl(self, pipeline_id: str, ttl_seconds: float) -> None:
        """Override the TTL for a specific pipeline."""
        self._ttl_overrides[pipeline_id] = ttl_seconds

    def touch(self, pipeline_id: str, at: Optional[datetime] = None) -> None:
        """Record that a pipeline was active at the given time (defaults to now)."""
        self._last_seen[pipeline_id] = at or datetime.utcnow()

    def ttl_for(self, pipeline_id: str) -> float:
        return self._ttl_overrides.get(pipeline_id, self._default_ttl)

    def check(self, pipeline_id: str, now: Optional[datetime] = None) -> bool:
        """Return True if the pipeline has expired."""
        now = now or datetime.utcnow()
        last = self._last_seen.get(pipeline_id)
        if last is None:
            return False
        ttl = self.ttl_for(pipeline_id)
        return (now - last).total_seconds() > ttl

    def evaluate(self, now: Optional[datetime] = None) -> ExpirerResult:
        """Evaluate all tracked pipelines and return an ExpirerResult."""
        now = now or datetime.utcnow()
        result = ExpirerResult()
        for pipeline_id, last in self._last_seen.items():
            ttl = self.ttl_for(pipeline_id)
            elapsed = (now - last).total_seconds()
            if elapsed > ttl:
                result.expired.append(
                    ExpireEntry(
                        pipeline_id=pipeline_id,
                        expired_at=last + timedelta(seconds=ttl),
                        reason=f"inactive for {elapsed:.1f}s (ttl={ttl}s)",
                        ttl_seconds=ttl,
                    )
                )
            else:
                result.active.append(pipeline_id)
        return result


def expire_snapshots(
    snapshots: List[PipelineSnapshot],
    default_ttl_seconds: float = 3600.0,
    ttl_overrides: Optional[Dict[str, float]] = None,
    now: Optional[datetime] = None,
) -> ExpirerResult:
    """Convenience function: expire snapshots based on their captured_at timestamp."""
    now = now or datetime.utcnow()
    overrides = ttl_overrides or {}
    expirer = PipelineExpirer(default_ttl_seconds=default_ttl_seconds)
    for pid, ttl in overrides.items():
        expirer.set_ttl(pid, ttl)
    for snap in snapshots:
        ts = getattr(snap, "captured_at", None) or now
        expirer.touch(snap.pipeline_id, at=ts)
    return expirer.evaluate(now=now)
