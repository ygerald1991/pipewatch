"""Pipeline throttler: tracks and enforces per-pipeline event rate limits."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional
import time


@dataclass
class ThrottlePolicy:
    default_max_events_per_second: float = 100.0
    overrides: Dict[str, float] = field(default_factory=dict)

    def max_events_for(self, pipeline_id: str) -> float:
        return self.overrides.get(pipeline_id, self.default_max_events_per_second)


@dataclass
class ThrottleResult:
    pipeline_id: str
    allowed: bool
    current_rate: float
    limit: float

    def __str__(self) -> str:
        status = "allowed" if self.allowed else "throttled"
        return (
            f"{self.pipeline_id}: {status} "
            f"(rate={self.current_rate:.2f}/s, limit={self.limit:.2f}/s)"
        )


class _TokenBucket:
    """Simple token-bucket for rate limiting."""

    def __init__(self, rate: float) -> None:
        self.rate = rate
        self._tokens: float = rate
        self._last: float = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self.rate, self._tokens + elapsed * self.rate)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    @property
    def current_rate(self) -> float:
        """Return the configured rate (events/s)."""
        return self.rate


class PipelineThrottler:
    def __init__(self, policy: Optional[ThrottlePolicy] = None) -> None:
        self._policy = policy or ThrottlePolicy()
        self._buckets: Dict[str, _TokenBucket] = {}

    def _bucket_for(self, pipeline_id: str) -> _TokenBucket:
        if pipeline_id not in self._buckets:
            limit = self._policy.max_events_for(pipeline_id)
            self._buckets[pipeline_id] = _TokenBucket(limit)
        return self._buckets[pipeline_id]

    def check(self, pipeline_id: str) -> ThrottleResult:
        bucket = self._bucket_for(pipeline_id)
        allowed = bucket.consume()
        limit = self._policy.max_events_for(pipeline_id)
        return ThrottleResult(
            pipeline_id=pipeline_id,
            allowed=allowed,
            current_rate=bucket.current_rate,
            limit=limit,
        )

    def pipeline_ids(self):
        return list(self._buckets.keys())
