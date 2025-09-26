from __future__ import annotations

from typing import Any, Dict, List

from sources.base import LeadSource
from sources.registry import register
from typing import Optional

from data_validator import DataValidator
from config.settings import get_settings
from google_searcher import GoogleSearcher
from data_extractor import LinkedInDataExtractor


class LinkedInPeopleSource(LeadSource):
    source_name = "linkedin_people_google"
    entity_type = "person"

    def __init__(self):
        # Lazy-init heavy/AI-dependent components to avoid failures in environments without AI keys
        self._searcher: Optional[GoogleSearcher] = None
        self._extractor: Optional[LinkedInDataExtractor] = None
        self._validator: Optional[DataValidator] = None

    def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
        if self._searcher is None:
            self._searcher = GoogleSearcher()
        if self._extractor is None:
            settings = get_settings()
            self._extractor = LinkedInDataExtractor(
                use_ai=settings.ai_enabled,
                openai_api_key=settings.openai_api_key,
                openai_model=None,
            )
        if self._validator is None:
            self._validator = DataValidator()

        results = self._searcher.search_linkedin_profiles(terms, max_results)
        profiles = self._extractor.extract_all_profiles(results)
        valid = self._validator.validate_all_profiles(profiles)
        unique = self._validator.remove_duplicates(valid)
        cleaned = [self._validator.clean_profile_data(p) for p in unique]
        return cleaned


def _register():
    register(LinkedInPeopleSource.source_name, LinkedInPeopleSource)


_register()


