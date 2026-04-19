from pipewatch.pipeline_auditor_session import AuditorSession
from pipewatch.auditor_session_reporter import render_audit_table, event_type_summary
from pipewatch.snapshot import PipelineSnapshot
from typing import List, Optional


def run_auditor_session(
    snapshots: List[PipelineSnapshot],
    pipeline_id: Optional[str] = None,
    show_summary: bool = False,
) -> str:
    """Run an audit session over the given snapshots and return a formatted report."""
    session = AuditorSession()
    for snapshot in snapshots:
        session.add_snapshot(snapshot)
    log = session.run()

    table = render_audit_table(log, pipeline_id=pipeline_id)
    if show_summary:
        summary = event_type_summary(log)
        return f"{table}\n\n{summary}"
    return table
