from __future__ import annotations

from typing import Any, Dict, List
import os

from sources.base import LeadSource
from sources.registry import register


class GoogleMapsCompaniesSource(LeadSource):
    source_name = "google_maps_companies"
    entity_type = "company"

    def __init__(self):
        # Scaffold only: no API calls. Optionally emit a small demo when DEMO=true
        pass

    def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
        if os.getenv("DEMO", "false").lower() == "true":
            # Minimal demo stub
            name = " ".join(terms) if terms else "Example Company"
            return [{
                "Company": name,
                "Company_Website": None,
                "Company_Domain": None,
                "address": None,
                "phone": None,
                "place_id": None,
            }]
        return []


def _register():
    register(GoogleMapsCompaniesSource.source_name, GoogleMapsCompaniesSource)


_register()



