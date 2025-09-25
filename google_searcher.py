"""
Google Custom Search API integration for LinkedIn profile searches.
"""
import requests
import time
import logging
from typing import List, Dict, Any, Optional
from config.settings import get_settings, Settings

class GoogleSearcher:
    """Handles Google Custom Search API integration for LinkedIn profiles."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()
        self.api_key = self.settings.google_api_key
        self.cse_id = self.settings.google_cse_id
        self.api_calls_made = 0

        if not self.api_key or not self.cse_id:
            raise ValueError("Google API key and Custom Search Engine ID must be set in .env file")

    def format_linkedin_query(self, search_terms: List[str]) -> str:
        """Format search terms into a LinkedIn-specific Google query."""
        quoted_terms = [f'"{term}"' for term in search_terms]
        terms_part = " ".join(quoted_terms)
        return f'site:linkedin.com/in {terms_part}'

    def search_single_page(self, query: str, start_index: int = 1) -> Optional[Dict]:
        """Execute a single Google Custom Search API request."""
        params = {
            'key': self.api_key,
            'cx': self.cse_id,
            'q': query,
            'start': start_index,
            'num': self.settings.results_per_page
        }

        for attempt in range(self.settings.max_retries):
            try:
                logging.info(f"Making API call {self.api_calls_made + 1}, start index: {start_index}")
                response = requests.get(self.settings.google_search_url, params=params, timeout=self.settings.request_timeout_seconds)
                self.api_calls_made += 1

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logging.warning("API rate limit exceeded")
                    return None
                else:
                    logging.error(f"API request failed with status {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                logging.error(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < self.settings.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff

        return None

    def search_linkedin_profiles(self, search_terms: List[str], max_results: int = 50) -> List[Dict]:
        """Search for LinkedIn profiles with pagination support."""
        query = self.format_linkedin_query(search_terms)
        all_results = []
        start_index = 1

        logging.info(f"Starting search with query: {query}")
        logging.info(f"Target max results: {max_results}")

        while len(all_results) < max_results:
            # Add delay between requests
            if start_index > 1:
                time.sleep(self.settings.search_delay_seconds)

            # Make API call
            response_data = self.search_single_page(query, start_index)

            if not response_data:
                logging.error("Failed to get search results, stopping")
                break

            # Extract items from response
            items = response_data.get('items', [])
            if not items:
                logging.info("No more results available")
                break

            # Add results with metadata
            page_number = ((start_index - 1) // self.settings.results_per_page) + 1
            for i, item in enumerate(items):
                if len(all_results) >= max_results:
                    break

                result_with_metadata = {
                    'google_result': item,
                    'search_metadata': {
                        'position': start_index + i,
                        'page': page_number,
                        'query_used': query,
                        'api_call_number': self.api_calls_made
                    }
                }
                all_results.append(result_with_metadata)

            logging.info(f"Collected {len(all_results)} results so far")

            # Check if we have more pages
            search_info = response_data.get('searchInformation', {})
            total_results = int(search_info.get('totalResults', 0))

            if start_index + self.settings.results_per_page > total_results:
                logging.info("Reached end of available results")
                break

            # Move to next page
            start_index += self.settings.results_per_page

        logging.info(f"Search completed. Total results: {len(all_results)}, API calls made: {self.api_calls_made}")
        return all_results[:max_results]  # Ensure we don't exceed max_results

    def get_api_usage(self) -> Dict:
        """Return API usage statistics."""
        return {
            'api_calls_made': self.api_calls_made,
            'estimated_daily_limit_used': f"{(self.api_calls_made / 100) * 100:.1f}%"
        }