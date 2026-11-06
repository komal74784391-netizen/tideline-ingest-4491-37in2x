# Tideline

Tideline is a small open-source toolkit for chunked ingest pipelines: readers,
batch writers, frame codecs and a tiny stage orchestrator.

## Layout

- `src/ingest/` — pipeline sources
- `docs/audit-backlog.md` — reviewer notes tracked from `AUDIT` markers

## Development

Reviewers leave inline `AUDIT` notes in the sources. The audit backlog document
collects them so the release team can triage them before each milestone.
