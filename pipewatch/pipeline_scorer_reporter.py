from typing import List, Optional
from pipewatch.pipeline_scorer import ScoringResult, PipelineScore


def _format_row(rank: int, score: PipelineScore) -> str:
    flag = " [!]" if score.score < 50.0 else ""
    return f"  {rank:>3}. {score.pipeline_id:<30} score={score.score:>6.1f}  grade={score.grade}{flag}"


def render_scorer_report(result: Optional[ScoringResult], top_n: int = 5) -> str:
    if result is None or not result.scores:
        return "No scoring data available."

    lines = ["Pipeline Scorer Report", "=" * 50]

    lines.append(f"\nTop {top_n} Pipelines:")
    for i, entry in enumerate(result.top(top_n), start=1):
        lines.append(_format_row(i, entry))

    lines.append(f"\nBottom {top_n} Pipelines:")
    for i, entry in enumerate(result.bottom(top_n), start=1):
        lines.append(_format_row(i, entry))

    avg = sum(s.score for s in result.scores) / len(result.scores)
    lines.append(f"\nTotal pipelines scored : {len(result.scores)}")
    lines.append(f"Average score          : {avg:.1f}")
    return "\n".join(lines)


def low_score_summary(result: Optional[ScoringResult], threshold: float = 50.0) -> str:
    if result is None:
        return "No data."
    low = [s for s in result.scores if s.score < threshold]
    if not low:
        return "All pipelines are above the score threshold."
    ids = ", ".join(s.pipeline_id for s in low)
    return f"{len(low)} pipeline(s) below {threshold}: {ids}"
