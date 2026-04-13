"""Tests for pipewatch.config_loader."""

import json
import os
import pytest

from pipewatch.config_loader import load_config, load_threshold_config
from pipewatch.thresholds import DEFAULT_THRESHOLDS


@pytest.fixture
def json_config_file(tmp_path):
    data = {
        "thresholds": {
            "error_rate_warning": 0.03,
            "error_rate_critical": 0.10,
            "throughput_warning": 40.0,
        }
    }
    p = tmp_path / "pipewatch.json"
    p.write_text(json.dumps(data))
    return str(p)


class TestLoadConfig:
    def test_returns_empty_dict_when_no_path(self):
        assert load_config(None) == {}

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.json")

    def test_loads_json_file(self, json_config_file):
        cfg = load_config(json_config_file)
        assert "thresholds" in cfg
        assert cfg["thresholds"]["error_rate_warning"] == 0.03

    def test_raises_on_unsupported_extension(self, tmp_path):
        p = tmp_path / "config.toml"
        p.write_text("[thresholds]")
        with pytest.raises(ValueError, match="Unsupported"):
            load_config(str(p))


class TestLoadThresholdConfig:
    def test_returns_defaults_when_no_path(self):
        cfg = load_threshold_config(None)
        assert cfg.error_rate_warning == DEFAULT_THRESHOLDS.error_rate_warning

    def test_loads_thresholds_from_json(self, json_config_file):
        cfg = load_threshold_config(json_config_file)
        assert cfg.error_rate_warning == 0.03
        assert cfg.error_rate_critical == 0.10
        assert cfg.throughput_warning == 40.0

    def test_unspecified_thresholds_use_defaults(self, json_config_file):
        cfg = load_threshold_config(json_config_file)
        assert cfg.throughput_critical == DEFAULT_THRESHOLDS.throughput_critical
