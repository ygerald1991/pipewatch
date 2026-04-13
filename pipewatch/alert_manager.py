"""AlertManager: evaluates rules against collected metrics and dispatches alerts."""

from typing import Callable, Dict, List, Optional
from pipewatch.alerts import Alert, AlertRule
from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetric

NotifyHandler = Callable[[Alert], None]


class AlertManager:
    """Evaluates registered alert rules against the latest pipeline metrics."""

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector
        self._rules: List[AlertRule] = []
        self._handlers: List[NotifyHandler] = []

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule to be evaluated on each check."""
        self._rules.append(rule)

    def add_handler(self, handler: NotifyHandler) -> None:
        """Register a notification handler (e.g. print, send email, post to Slack)."""
        self._handlers.append(handler)

    def check(self, pipeline_id: str) -> List[Alert]:
        """Evaluate all rules for the given pipeline and return triggered alerts."""
        metric = self._collector.latest(pipeline_id)
        if metric is None:
            return []

        triggered: List[Alert] = []
        for rule in self._rules:
            if rule.pipeline_id != pipeline_id:
                continue
            alert = rule.evaluate(metric)
            if alert:
                triggered.append(alert)
                self._dispatch(alert)

        return triggered

    def check_all(self) -> Dict[str, List[Alert]]:
        """Evaluate rules for every pipeline that has recorded metrics."""
        pipeline_ids = {rule.pipeline_id for rule in self._rules}
        return {pid: self.check(pid) for pid in pipeline_ids}

    def _dispatch(self, alert: Alert) -> None:
        for handler in self._handlers:
            handler(alert)


def stdout_handler(alert: Alert) -> None:
    """Simple handler that prints alerts to stdout."""
    print(str(alert))
