"""Unit tests for AlertManager."""

import pytest
from pipewatch.alert_manager import AlertManager, stdout_handler
from pipewatch.alerts import AlertSeverity, high_error_rate_rule, pipeline_down_rule
from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric, PipelineStatus


def make_metric(
    pipeline_id="pipe-1",
    total=100,
    failed=0,
    duration=10.0,
    status=PipelineStatus.OK,
) -> PipelineMetric:
    return PipelineMetric(
        pipeline_id=pipeline_id,
        total_records=total,
        failed_records=failed,
        duration_seconds=duration,
        status=status,
    )


@pytest.fixture
def setup_manager():
    collector = MetricsCollector()
    manager = AlertManager(collector)
    return collector, manager


def test_no_alerts_when_no_metrics(setup_manager):
    collector, manager = setup_manager
    manager.add_rule(high_error_rate_rule("pipe-1"))
    alerts = manager.check("pipe-1")
    assert alerts == []


def test_no_alerts_when_healthy(setup_manager):
    collector, manager = setup_manager
    collector.record(make_metric(total=100, failed=2))
    manager.add_rule(high_error_rate_rule("pipe-1", threshold=0.1))
    alerts = manager.check("pipe-1")
    assert alerts == []


def test_alert_triggered_on_high_error_rate(setup_manager):
    collector, manager = setup_manager
    collector.record(make_metric(total=100, failed=20))
    manager.add_rule(high_error_rate_rule("pipe-1", threshold=0.1))
    alerts = manager.check("pipe-1")
    assert len(alerts) == 1
    assert alerts[0].rule_name == "high_error_rate"


def test_handler_called_on_alert(setup_manager):
    collector, manager = setup_manager
    collector.record(make_metric(status=PipelineStatus.FAILED))
    manager.add_rule(pipeline_down_rule("pipe-1"))

    received = []
    manager.add_handler(lambda a: received.append(a))
    manager.check("pipe-1")

    assert len(received) == 1
    assert received[0].rule_name == "pipeline_down"


def test_check_all_returns_per_pipeline_results(setup_manager):
    collector, manager = setup_manager
    collector.record(make_metric(pipeline_id="pipe-1", total=100, failed=50))
    collector.record(make_metric(pipeline_id="pipe-2", total=100, failed=0))
    manager.add_rule(high_error_rate_rule("pipe-1", threshold=0.1))
    manager.add_rule(high_error_rate_rule("pipe-2", threshold=0.1))

    results = manager.check_all()
    assert len(results["pipe-1"]) == 1
    assert len(results["pipe-2"]) == 0


def test_rules_scoped_to_pipeline(setup_manager):
    collector, manager = setup_manager
    collector.record(make_metric(pipeline_id="pipe-1", total=100, failed=50))
    manager.add_rule(high_error_rate_rule("pipe-2", threshold=0.1))
    alerts = manager.check("pipe-1")
    assert alerts == []
