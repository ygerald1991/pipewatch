from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Iterator, List, Optional
from pipewatch.snapshot import PipelineSnapshot


@dataclass
class StreamChunk:
    index: int
    pipeline_id: str
    snapshot: PipelineSnapshot

    def __str__(self) -> str:
        return f"StreamChunk(index={self.index}, pipeline={self.pipeline_id})"


@dataclass
class StreamResult:
    chunks: List[StreamChunk] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.chunks)

    def pipeline_ids(self) -> List[str]:
        seen = []
        for c in self.chunks:
            if c.pipeline_id not in seen:
                seen.append(c.pipeline_id)
        return seen

    def for_pipeline(self, pipeline_id: str) -> List[StreamChunk]:
        return [c for c in self.chunks if c.pipeline_id == pipeline_id]


def stream_snapshots(
    snapshots: List[PipelineSnapshot],
    chunk_size: int = 10,
    on_chunk: Optional[Callable[[StreamChunk], None]] = None,
) -> Iterator[StreamChunk]:
    for index, snapshot in enumerate(snapshots):
        chunk = StreamChunk(
            index=index,
            pipeline_id=snapshot.pipeline_id,
            snapshot=snapshot,
        )
        if on_chunk is not None:
            on_chunk(chunk)
        yield chunk


def collect_stream(
    snapshots: List[PipelineSnapshot],
    chunk_size: int = 10,
    on_chunk: Optional[Callable[[StreamChunk], None]] = None,
) -> StreamResult:
    result = StreamResult()
    for chunk in stream_snapshots(snapshots, chunk_size=chunk_size, on_chunk=on_chunk):
        result.chunks.append(chunk)
    return result
