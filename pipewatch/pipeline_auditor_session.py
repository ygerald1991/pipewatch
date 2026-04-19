from pipewatch.pipeline_auditor import AuditLog, AuditEvent
from pipewatch.snapshot import PipelineSnapshot
from typing import List, Optional


class AuditorSession:
    def __init__(self) -> None:
        self._log = AuditLog()
        self._snapshots: List[PipelineSnapshot] = []

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def record_event(self, pipeline_id: str, event_type: str, message: str) -> AuditEvent:
        return self._log.record(pipeline_id, event_type, message)

    def run(self) -> AuditLog:
        for snapshot in self._snapshots:
            if snapshot.error_rate is not None and snapshot.error_rate >= 0.2:
                self._log.record(snapshot.pipeline_id, "critical_error_rate",
                                 f"Error rate {snapshot.error_rate:.2%} exceeds critical threshold")
            elif snapshot.error_rate is not None and snapshot.error_rate >= 0.1:
                self._log.record(snapshot.pipeline_id, "high_error_rate",
                                 f"Error rate {snapshot.error_rate:.2%} exceeds warning threshold")
            else:
                self._log.record(snapshot.pipeline_id, "healthy",
                                 "Pipeline operating within normal parameters")
        return self._log

    @property
    def pipeline_ids(self) -> List[str]:
        return list({s.pipeline_id for s in self._snapshots})

    @property
    def log(self) -> AuditLog:
        return self._log
