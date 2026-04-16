from __future__ import annotations
from pipewatch.pipeline_streamer import StreamResult


def _format_row(index: int, pipeline_id: str, error_rate: str, throughput: str) -> str:
    return f"  {index:<6} {pipeline_id:<24} {error_rate:<14} {throughput}"


def render_stream_table(result: StreamResult) -> str:
    if not result.chunks:
        return "No stream chunks available."

    lines = [
        "Stream Report",
        "-" * 60,
        _format_row("#", "Pipeline", "Error Rate", "Throughput"),
        "-" * 60,
    ]

    for chunk in result.chunks:
        s = chunk.snapshot
        er = f"{s.error_rate:.2%}" if s.error_rate is not None else "n/a"
        tp = f"{s.throughput:.1f}/s" if s.throughput is not None else "n/a"
        lines.append(_format_row(chunk.index, chunk.pipeline_id, er, tp))

    lines.append("-" * 60)
    lines.append(f"  Total chunks: {result.total}")
    return "\n".join(lines)


def stream_pipeline_summary(result: StreamResult) -> str:
    ids = result.pipeline_ids()
    if not ids:
        return "No pipelines streamed."
    parts = [f"{pid} ({len(result.for_pipeline(pid))} chunks)" for pid in ids]
    return "Pipelines streamed: " + ", ".join(parts)
