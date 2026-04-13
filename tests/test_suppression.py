"""Tests for the alert suppression module."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.suppression import SuppressionStore, SuppressionRule, apply_suppression


@pytest.fixture
def store() -> SuppressionStore:
    return SuppressionStore()


class TestSuppressionStore:
    def test_new_store_has_no_active_suppressions(self, store):
        assert store.active_suppressions() == {}

    def test_is_suppressed_returns_false_when_not_set(self, store):
        assert store.is_suppressed("pipe-1", "high_error_rate") is False

    def test_suppress_marks_alert_as_suppressed(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=60)
        assert store.is_suppressed("pipe-1", "high_error_rate") is True

    def test_suppression_does_not_affect_other_pipeline(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=60)
        assert store.is_suppressed("pipe-2", "high_error_rate") is False

    def test_suppression_does_not_affect_other_rule(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=60)
        assert store.is_suppressed("pipe-1", "low_throughput") is False

    def test_expired_suppression_returns_false(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=1)
        future = datetime.utcnow() + timedelta(seconds=10)
        with patch("pipewatch.suppression.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future
            assert store.is_suppressed("pipe-1", "high_error_rate") is False

    def test_expired_suppression_is_removed_from_store(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=1)
        future = datetime.utcnow() + timedelta(seconds=10)
        with patch("pipewatch.suppression.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future
            store.is_suppressed("pipe-1", "high_error_rate")
        assert ("pipe-1", "high_error_rate") not in store._suppressed

    def test_release_removes_suppression(self, store):
        store.suppress("pipe-1", "high_error_rate", duration_seconds=60)
        store.release("pipe-1", "high_error_rate")
        assert store.is_suppressed("pipe-1", "high_error_rate") is False

    def test_release_on_missing_key_does_not_raise(self, store):
        store.release("pipe-x", "nonexistent")  # should not raise

    def test_clear_removes_all_suppressions(self, store):
        store.suppress("pipe-1", "rule-a", 60)
        store.suppress("pipe-2", "rule-b", 60)
        store.clear()
        assert store.active_suppressions() == {}

    def test_active_suppressions_excludes_expired(self, store):
        store.suppress("pipe-1", "rule-a", 60)
        store.suppress("pipe-2", "rule-b", 1)
        future = datetime.utcnow() + timedelta(seconds=10)
        with patch("pipewatch.suppression.datetime") as mock_dt:
            mock_dt.utcnow.return_value = future
            active = store.active_suppressions()
        assert ("pipe-2", "rule-b") not in active


class TestApplySuppression:
    def test_returns_false_when_not_suppressed_and_no_auto(self, store):
        result = apply_suppression(store, "pipe-1", "rule-a")
        assert result is False

    def test_returns_true_when_already_suppressed(self, store):
        store.suppress("pipe-1", "rule-a", 60)
        result = apply_suppression(store, "pipe-1", "rule-a")
        assert result is True

    def test_auto_suppress_registers_suppression_on_first_call(self, store):
        apply_suppression(store, "pipe-1", "rule-a", auto_suppress_seconds=30)
        assert store.is_suppressed("pipe-1", "rule-a") is True

    def test_auto_suppress_returns_false_on_first_call(self, store):
        result = apply_suppression(store, "pipe-1", "rule-a", auto_suppress_seconds=30)
        assert result is False

    def test_auto_suppress_zero_does_not_register(self, store):
        apply_suppression(store, "pipe-1", "rule-a", auto_suppress_seconds=0)
        assert store.is_suppressed("pipe-1", "rule-a") is False
