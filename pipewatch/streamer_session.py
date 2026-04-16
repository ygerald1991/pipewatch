from __future__ import annotations
from typing import Callable, List, Optional
from pipewatch.snapshot import PipelineSnapshot
from pipewatch.pipeline_streamer import StreamChunk, StreamResult, collect_stream


class StreamerSession:
    def __init__(self, chunk_size: int = 10) -> None:
        self._chunk_size = chunk_size
        self._snapshots: List[PipelineSnapshot] = []
        self._handlers: List[Callable[[StreamChunk], None]] = []

    def add_snapshot(self, snapshot: PipelineSnapshot) -> None:
        self._snapshots.append(snapshot)

    def add_handler(self, handler: Callable[[StreamChunk], None]) -> None:
        self._handlers.append(handler)

    def pipeline_ids(self) -> List[str]:
        seen = []
        for s in self._snapshots:
            if s.pipeline_id not in seen:
                seen.append(s.pipeline_id)
        return seen

    def _dispatch(self, chunk: StreamChunk) -> None:
        for handler in self._handlers:
            handler(chunk)

    def run(self) -> StreamResult:
        return collect_stream(
            self._snapshots,
            chunk_size=self._chunk_size,
            on_chunk=self._dispatch,
        )
