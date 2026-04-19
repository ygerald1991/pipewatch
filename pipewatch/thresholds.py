"""Default and configurable thresholds for pipeline health evaluation."""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ThresholdConfig:
    """Holds threshold values for pipeline health metrics."""

    error_rate_warning: float = 0.05   # 5%
    error_rate_critical: float = 0.15  # 15%
    throughput_warning: float = 50.0   # records/sec
    throughput_critical: float = 10.0  # records/sec
    latency_warning: float = 2.0       # seconds
    latency_critical: float = 5.0      # seconds

    overrides: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def for_pipeline(self, pipeline_id: str) -> "ThresholdConfig":
        """Return a ThresholdConfig with pipeline-specific overrides applied."""
        if pipeline_id not in self.overrides:
            return self
        overridden = ThresholdConfig(
            error_rate_warning=self.error_rate_warning,
            error_rate_critical=self.error_rate_critical,
            throughput_warning=self.throughput_warning,
            throughput_critical=self.throughput_critical,
            latency_warning=self.latency_warning,
            latency_critical=self.latency_critical,
        )
        for key, value in self.overrides[pipeline_id].items():
            if hasattr(overridden, key):
                setattr(overridden, key, value)
        return overridden

    def validate(self) -> None:
        """Raise ValueError if any threshold values are logically inconsistent."""
        if self.error_rate_warning >= self.error_rate_critical:
            raise ValueError(
                f"error_rate_warning ({self.error_rate_warning}) must be "
                f"less than error_rate_critical ({self.error_rate_critical})"
            )
        if self.throughput_warning <= self.throughput_critical:
            raise ValueError(
                f"throughput_warning ({self.throughput_warning}) must be "
                f"greater than throughput_critical ({self.throughput_critical})"
            )
        if self.latency_warning >= self.latency_critical:
            raise ValueError(
                f"latency_warning ({self.latency_warning}) must be "
                f"less than latency_critical ({self.latency_critical})"
            )


DEFAULT_THRESHOLDS = ThresholdConfig()


def load_thresholds(config: Optional[Dict] = None) -> ThresholdConfig:
    """Build a ThresholdConfig from a plain dict (e.g. loaded from YAML/JSON)."""
    if not config:
        return DEFAULT_THRESHOLDS
    overrides = config.pop("overrides", {})
    cfg = ThresholdConfig(**{k: v for k, v in config.items() if hasattr(ThresholdConfig, k)})
    cfg.overrides = overrides
    cfg.validate()
    return cfg
