from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_stepper import StepperResult


def _format_row(result: StepperResult) -> str:
    current = result.current_step
    step_label = f"{current.step_index}:{current.step_name}" if current else "—"
    return f"  {result.pipeline_id:<30} steps={result.total_steps:<5} current={step_label}"


def render_stepper_table(results: List[StepperResult]) -> str:
    if not results:
        return "No stepper results."
    lines = [
        "Pipeline Stepper Report",
        "-" * 60,
        f"  {'Pipeline':<30} {'Steps':<10} {'Current Step'}",
        "-" * 60,
    ]
    for r in results:
        lines.append(_format_row(r))
    lines.append("-" * 60)
    return "\n".join(lines)


def step_progress_summary(results: List[StepperResult]) -> str:
    if not results:
        return "No pipelines tracked."
    total_pipelines = len(results)
    total_steps = sum(r.total_steps for r in results)
    avg = total_steps / total_pipelines if total_pipelines else 0.0
    return (
        f"{total_pipelines} pipeline(s) tracked, "
        f"{total_steps} total step(s), "
        f"{avg:.1f} avg steps/pipeline."
    )
