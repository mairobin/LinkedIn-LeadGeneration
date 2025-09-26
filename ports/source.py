from __future__ import annotations

from typing import Any, Dict, List, Literal, Protocol


EntityType = Literal["person", "company"]


class SourcePort(Protocol):
    source_name: str
    entity_type: EntityType

    def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
        ...

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        ...


