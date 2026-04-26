"""Load pipewatch configuration from a YAML or JSON file."""

import json
import os
from typing import Any, Dict, Optional

from pipewatch.thresholds import ThresholdConfig, load_thresholds

_SUPPORTED_EXTENSIONS = (".json", ".yaml", ".yml")


def _read_file(path: str) -> Dict[str, Any]:
    ext = os.path.splitext(path)[1].lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported config file format: '{ext}'. "
            f"Supported formats: {', '.join(_SUPPORTED_EXTENSIONS)}"
        )
    with open(path, "r", encoding="utf-8") as fh:
        if ext == ".json":
            return json.load(fh)
        else:  # .yaml / .yml
            try:
                import yaml
                return yaml.safe_load(fh) or {}
            except ImportError as exc:
                raise ImportError(
                    "PyYAML is required to load YAML config files: pip install pyyaml"
                ) from exc


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Load raw config dict from *path*, or return empty dict if path is None."""
    if path is None:
        return {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    return _read_file(path)


def load_threshold_config(path: Optional[str] = None) -> ThresholdConfig:
    """Convenience wrapper: load config file and return a ThresholdConfig."""
    raw = load_config(path)
    threshold_section = raw.get("thresholds", {})
    return load_thresholds(threshold_section)
