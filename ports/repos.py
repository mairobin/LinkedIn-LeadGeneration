from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, Tuple


class PeopleRepoPort(Protocol):
    def upsert_person(
        self,
        linkedin_profile: str,
        first_name: Optional[str],
        last_name: Optional[str],
        title_current: Optional[str],
        email: Optional[str],
        location_text: Optional[str],
        connections_linkedin: Optional[int] = None,
        followers_linkedin: Optional[int] = None,
        website_info: Optional[str] = None,
        phone_info: Optional[str] = None,
        info_raw: Optional[str] = None,
        insights_text: Optional[str] = None,
        lookup_date: Optional[str] = None,
        source_name: Optional[str] = None,
        source_query: Optional[str] = None,
    ) -> int:
        ...

    def link_person_to_company(self, linkedin_profile: str, company_id: int) -> None:
        ...


class CompaniesRepoPort(Protocol):
    def upsert_by_domain(
        self,
        name: Optional[str],
        domain: Optional[str],
        website: Optional[str],
        source_name: Optional[str] = None,
        source_query: Optional[str] = None,
    ) -> int:
        ...

    def update_enrichment(self, company_id: int, fields: Dict[str, Any]) -> None:
        ...

    def select_pending_enrichment(self, limit: int = 50) -> List[Tuple]:
        ...


