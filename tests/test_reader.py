"""Smoke tests for the chunked reader."""

import io

from src.ingest.reader import ChunkReader, ReaderConfig, parse_frames


def test_chunk_reader_splits_payload():
    payload = b"x" * 40
    reader = ChunkReader(io.BytesIO(payload), ReaderConfig(chunk_bytes=10))
    assert [len(c) for c in reader] == [10, 10, 10, 10]


def test_parse_frames_skips_empty_chunks():
    frames = list(parse_frames(b"abcd"))
    assert frames == [b"abcd"]
