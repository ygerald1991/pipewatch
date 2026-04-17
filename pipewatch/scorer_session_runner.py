from typing import List, Dict, Any
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_scorer_session import ScorerSession
from pipewatch.pipeline_scorer_reporter import render_scorer_report, low_score_summary


def run_scorer_session(
    snapshots: List[PipelineSnapshot],
    top_n: int = 5,
    low_score_threshold: float = 50.0,
) -> Dict[str, Any]:
    """Run a full scorer session and return a structured result dict."""
    session = ScorerSession()
    for snap in snapshots:
        session.add_snapshot(snap)

    result = session.run()

    return {
        "result": result,
        "report": render_scorer_report(result, top_n=top_n),
        "low_score_summary": low_score_summary(result, threshold=low_score_threshold),
        "top": session.top(top_n),
        "bottom": session.bottom(top_n),
        "pipeline_count": len(session.pipeline_ids()),
    }
