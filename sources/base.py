from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Protocol, Optional


EntityType = Literal["person", "company"]


@dataclass
class PersonRecord:
    profile_url: str
    name: Optional[str] = None
    current_position: Optional[str] = None
    company: Optional[str] = None
    email: Optional[str] = None
    location: Optional[str] = None
    connection_count: Optional[int] = None
    follower_count: Optional[int] = None
    summary: Optional[str] = None
    company_website: Optional[str] = None
    lookup_date: Optional[str] = None


@dataclass
class CompanyRecord:
    name: str
    website: Optional[str] = None
    domain: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    place_id: Optional[str] = None
    size_employees: Optional[int] = None


class LeadSource(Protocol):
    source_name: str
    entity_type: EntityType

    def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
        ...

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw



