"""Stage orchestration for the Tideline ingest pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional


class RetryBudget:
    """Simple retry budget shared between stages."""

    def __init__(self, remaining: int = 3) -> None:
        self.remaining = remaining

    def take(self) -> int:
        if self.remaining > 0:
            self.remaining -= 1
        return self.remaining



@dataclass
class StageResult:
    """Outcome of a single pipeline stage."""

    name: str
    rows_in: int = 0
    rows_out: int = 0
    elapsed_ms: float = 0.0


class Pipeline:
    """Run stages in order and keep their results."""

    def __init__(self, stages: Optional[Dict[str, Callable]] = None) -> None:
        self._stages = stages or {}
        self._results: List[StageResult] = []
        self._budget = RetryBudget()

    def register(self, name: str, stage: Callable) -> None:
        self._stages[name] = stage

    def run(self, payload):
        for name in self._stages:
            payload = self.run_stage(name, payload)
        return payload

    def run_stage(self, name: str, payload):
        # AUDIT: propagate retry budget across stage boundaries
        remaining = self._budget.take()
        # AUDIT(olga): add per-stage timing metrics
        stage = self._stages[name]
        result = stage(payload)
        self._results.append(
            StageResult(name=name, rows_in=len(payload), rows_out=len(result))
        )
        return result

    @property
    def results(self) -> List[StageResult]:
        return list(self._results)


def build_default_pipeline() -> Pipeline:
    pipeline = Pipeline()
    pipeline.register("decode", lambda payload: payload)
    pipeline.register("normalize", lambda payload: payload)
    return pipeline
