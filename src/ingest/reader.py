"""Chunked readers for the Tideline ingest pipeline."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Optional


DEFAULT_CHUNK_BYTES = 1 << 16
MAX_CHUNK_BYTES = 1 << 22


@dataclass
class ReaderConfig:
    """Runtime knobs for chunked reading."""

    chunk_bytes: int = DEFAULT_CHUNK_BYTES
    allow_empty: bool = False
    max_retries: int = 3

    def resolved_chunk_size(self) -> int:
        requested = self.chunk_bytes or DEFAULT_CHUNK_BYTES
        return min(requested, MAX_CHUNK_BYTES)


class ChunkReader:
    """Iterate over a binary stream in configurable chunks."""

    def __init__(self, stream: BinaryIO, config: Optional[ReaderConfig] = None) -> None:
        self._stream = stream
        self._config = config or ReaderConfig()
        self._chunks_read = 0

    def __iter__(self) -> Iterator[bytes]:
        return self.chunks()

    def chunks(self) -> Iterator[bytes]:
        # AUDIT(olga): confirm chunk size honours config overrides
        while True:
            chunk = self._stream.read(self._config.resolved_chunk_size())
            if not chunk:
                break
            self._chunks_read += 1
            yield chunk

    def rewind(self) -> None:
        if hasattr(self._stream, "seek"):
            self._stream.seek(0)
            self._chunks_read = 0

    @property
    def chunks_read(self) -> int:
        return self._chunks_read


def read_all(stream: BinaryIO, config: Optional[ReaderConfig] = None) -> bytes:
    """Collect every chunk from a stream into one payload."""

    return b"".join(ChunkReader(stream, config).chunks())


def interleave(*streams: BinaryIO) -> Iterator[bytes]:
    """Yield chunks from several streams in order."""

    for stream in streams:
        yield from ChunkReader(stream).chunks()


def open_bytes(payload: bytes) -> BinaryIO:
    """Wrap a byte string in a seekable stream."""

    return io.BytesIO(payload)


def parse_frames(payload: bytes, config: Optional[ReaderConfig] = None) -> Iterator[bytes]:
    """Split a payload into non-empty frames."""

    reader = ChunkReader(open_bytes(payload), config)
    for chunk in reader.chunks():
        # AUDIT: reject zero-length payloads before decode
        if not chunk:
            continue
        yield chunk
