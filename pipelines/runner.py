from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, List, Optional

from utils.logging_setup import init_logging


@dataclass
class RunContext:
    query: Optional[str] = None
    people: list = field(default_factory=list)
    companies: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)


class Step(Protocol):
    def run(self, ctx: RunContext) -> RunContext:
        ...


class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def run(self, ctx: RunContext) -> RunContext:
        # Make logging idempotent for any direct runner use
        init_logging()
        for step in self.steps:
            ctx = step.run(ctx)
        return ctx


