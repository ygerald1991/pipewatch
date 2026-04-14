"""Rate limiter for controlling alert notification frequency per pipeline."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


@dataclass
class RateLimitPolicy:
    """Defines how frequently alerts may fire for a given pipeline/severity."""
    max_alerts: int = 5
    window_seconds: int = 300  # 5-minute window by default
    pipeline_overrides: Dict[str, Tuple[int, int]] = field(default_factory=dict)

    def limits_for(self, pipeline_id: str) -> Tuple[int, int]:
        """Return (max_alerts, window_seconds) for the given pipeline."""
        if pipeline_id in self.pipeline_overrides:
            return self.pipeline_overrides[pipeline_id]
        return self.max_alerts, self.window_seconds


@dataclass
class _BucketState:
    count: int = 0
    window_start: datetime = field(default_factory=datetime.utcnow)


class RateLimiter:
    """Tracks alert counts per (pipeline_id, severity) and enforces rate limits."""

    def __init__(self, policy: Optional[RateLimitPolicy] = None) -> None:
        self._policy = policy or RateLimitPolicy()
        self._buckets: Dict[str, _BucketState] = {}

    def _bucket_key(self, pipeline_id: str, severity: str) -> str:
        return f"{pipeline_id}::{severity}"

    def is_allowed(self, pipeline_id: str, severity: str, now: Optional[datetime] = None) -> bool:
        """Return True if an alert for this pipeline/severity is within rate limits."""
        now = now or datetime.utcnow()
        max_alerts, window_seconds = self._policy.limits_for(pipeline_id)
        key = self._bucket_key(pipeline_id, severity)

        if key not in self._buckets:
            self._buckets[key] = _BucketState(window_start=now)

        bucket = self._buckets[key]
        window_delta = timedelta(seconds=window_seconds)

        if now - bucket.window_start >= window_delta:
            bucket.count = 0
            bucket.window_start = now

        return bucket.count < max_alerts

    def record(self, pipeline_id: str, severity: str, now: Optional[datetime] = None) -> None:
        """Record that an alert was dispatched for this pipeline/severity."""
        now = now or datetime.utcnow()
        key = self._bucket_key(pipeline_id, severity)
        if key not in self._buckets:
            self._buckets[key] = _BucketState(window_start=now)
        self._buckets[key].count += 1

    def reset(self, pipeline_id: str, severity: str) -> None:
        """Clear the rate limit bucket for a pipeline/severity pair."""
        key = self._bucket_key(pipeline_id, severity)
        self._buckets.pop(key, None)

    def current_count(self, pipeline_id: str, severity: str) -> int:
        """Return the current alert count within the active window."""
        key = self._bucket_key(pipeline_id, severity)
        return self._buckets[key].count if key in self._buckets else 0
