from typing import Dict
from pipewatch.pipeline_circuit_breaker import BreakerResult


def _format_row(result: BreakerResult) -> str:
    indicator = "[OPEN]" if result.tripped else "[OK]  "
    return f"  {indicator} {result.pipeline_id:<30} failures={result.failure_count}  state={result.state}"


def render_circuit_breaker_table(results: Dict[str, BreakerResult]) -> str:
    if not results:
        return "No circuit breaker data available."
    lines = ["Circuit Breaker Status", "=" * 55]
    for result in results.values():
        lines.append(_format_row(result))
    return "\n".join(lines)


def tripped_summary(results: Dict[str, BreakerResult]) -> str:
    tripped = [r for r in results.values() if r.tripped]
    if not tripped:
        return "All circuits closed."
    ids = ", ".join(r.pipeline_id for r in tripped)
    return f"{len(tripped)} circuit(s) open: {ids}"
