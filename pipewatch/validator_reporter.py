from pipewatch.pipeline_validator import ValidationResult


def _format_row(result: ValidationResult) -> str:
    status = "PASS" if result.passed else "FAIL"
    violations = "; ".join(result.violations) if result.violations else "none"
    return f"  {result.pipeline_id:<30} {status:<6}  {violations}"


def render_validation_table(results: list) -> str:
    if not results:
        return "No validation results."
    header = f"  {'Pipeline':<30} {'Status':<6}  Violations"
    separator = "  " + "-" * 70
    rows = [_format_row(r) for r in results]
    return "\n".join([header, separator] + rows)


def failed_summary(results: list) -> str:
    failed = [r for r in results if not r.passed]
    if not failed:
        return "All pipelines passed validation."
    lines = [f"  {r.pipeline_id}: {'; '.join(r.violations)}" for r in failed]
    return f"{len(failed)} pipeline(s) failed validation:\n" + "\n".join(lines)
