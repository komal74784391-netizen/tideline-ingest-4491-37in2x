# Tideline — Audit Backlog

Notes filed by reviewers during the 2.x hardening pass. Keep this list in sync
with the `AUDIT` markers in the sources. Entries are ordered by path, then by
line; the note text follows the convention the reviewers have always used here.

- [ ] **src/ingest/codec.py:14** - (nadia): normalize locale before checksum compare
- [ ] **src/ingest/codec.py:52** - skip checksum on passthrough frames
- [ ] **src/ingest/pipeline.py:35** - (olga): add per-stage timing metrics
- [ ] **src/ingest/reader.py:39** - (olga): confirm chunk size honours config overrides
- [ ] **src/ingest/reader.py:81** - reject zero-length payloads before decode
- [ ] **src/ingest/writer.py:32** - drop legacy compression fallback
- [ ] **src/ingest/writer.py:68** - (ravi): guard against partial flush on retry
