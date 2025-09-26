from __future__ import annotations

from typing import Any, Dict, List

from sources.base import LeadSource
from sources.registry import register


class LinkedInCompaniesGoogleSource(LeadSource):
    source_name = "linkedin_companies_google"
    entity_type = "company"

    def __init__(self):
        # Scaffold only: no implementation yet
        pass

    def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
        # Intentionally empty: to be implemented later via Google CSE targeting linkedin.com/company/
        return []


def _register():
    register(LinkedInCompaniesGoogleSource.source_name, LinkedInCompaniesGoogleSource)


_register()



