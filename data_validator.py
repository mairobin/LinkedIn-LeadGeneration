import logging
import re
from datetime import datetime
from typing import Dict, List, Any
from urllib.parse import urlparse
from config import REQUIRED_FIELDS, OPTIONAL_FIELDS

class DataValidator:
    def __init__(self):
        self.validation_stats = {
            'total_profiles': 0,
            'valid_profiles': 0,
            'invalid_profiles': 0,
            'validation_errors': []
        }

    def validate_linkedin_url(self, url: str) -> bool:
        """Validate LinkedIn URL format."""
        if not url or not isinstance(url, str):
            return False

        try:
            parsed = urlparse(url)
            return (
                parsed.scheme in ['http', 'https'] and
                'linkedin.com' in parsed.netloc.lower() and
                '/in/' in parsed.path
            )
        except Exception:
            return False

    def validate_name(self, name: str) -> bool:
        """Validate person name."""
        if not name or not isinstance(name, str):
            return False

        # Basic validation: should have reasonable length
        name = name.strip()
        return (
            len(name) >= 2 and
            len(name) <= 200 and
            not name.startswith(('http', 'www', '@'))  # Basic sanity checks
        )

    def validate_required_fields(self, profile: Dict) -> List[str]:
        """Check if all required fields are present and valid."""
        errors = []

        for field in REQUIRED_FIELDS:
            if field not in profile or not profile[field]:
                errors.append(f"Missing required field: {field}")
                continue

            # Field-specific validation
            if field == 'profile_url' and not self.validate_linkedin_url(profile[field]):
                errors.append(f"Invalid LinkedIn URL: {profile[field]}")

            if field == 'name' and not self.validate_name(profile[field]):
                errors.append(f"Invalid name format: {profile[field]}")

        return errors

    def validate_optional_fields(self, profile: Dict) -> List[str]:
        """Validate optional fields if present."""
        warnings = []

        for field in OPTIONAL_FIELDS:
            if field in profile and profile[field]:
                value = profile[field]

                # Field-specific validation for optional fields
                if field == 'current_position' and len(str(value)) > 200:
                    warnings.append(f"Current position field too long: {len(str(value))} characters")

                if field == 'company' and len(str(value)) > 200:
                    warnings.append(f"Company field too long: {len(str(value))} characters")

                if field == 'location' and len(str(value)) > 200:
                    warnings.append(f"Location field too long: {len(str(value))} characters")

                if field == 'skills' and len(str(value)) > 300:
                    warnings.append(f"Skills field too long: {len(str(value))} characters")

                if field == 'summary' and len(str(value)) > 2000:
                    warnings.append(f"Summary field too long: {len(str(value))} characters")

        return warnings

    def validate_metadata_structure(self, profile: Dict) -> List[str]:
        """Validate the streamlined profile structure."""
        errors = []

        # For the streamlined format, we only need to validate that we have the core fields
        # No complex metadata validation needed anymore

        return errors

    def validate_profile_data(self, profile: Dict) -> Dict[str, Any]:
        """Validate a single profile and return validation results."""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'profile': profile
        }

        # Validate required fields
        required_errors = self.validate_required_fields(profile)
        validation_result['errors'].extend(required_errors)

        # Validate optional fields
        optional_warnings = self.validate_optional_fields(profile)
        validation_result['warnings'].extend(optional_warnings)

        # Validate metadata structure
        metadata_errors = self.validate_metadata_structure(profile)
        validation_result['errors'].extend(metadata_errors)

        # Determine if profile is valid
        validation_result['is_valid'] = len(validation_result['errors']) == 0

        # Update stats
        self.validation_stats['total_profiles'] += 1
        if validation_result['is_valid']:
            self.validation_stats['valid_profiles'] += 1
        else:
            self.validation_stats['invalid_profiles'] += 1
            self.validation_stats['validation_errors'].extend(validation_result['errors'])

        return validation_result

    def validate_all_profiles(self, profiles: List[Dict]) -> List[Dict]:
        """Validate all profiles and return only valid ones."""
        valid_profiles = []

        logging.info(f"Starting validation of {len(profiles)} profiles")

        for i, profile in enumerate(profiles):
            validation_result = self.validate_profile_data(profile)

            if validation_result['is_valid']:
                valid_profiles.append(profile)
                if validation_result['warnings']:
                    logging.warning(f"Profile {i+1} has warnings: {validation_result['warnings']}")
            else:
                logging.error(f"Profile {i+1} validation failed: {validation_result['errors']}")

        logging.info(f"Validation completed. Valid: {len(valid_profiles)}, "
                    f"Invalid: {len(profiles) - len(valid_profiles)}")

        return valid_profiles

    def format_output_structure(self, profiles: List[Dict], search_metadata: Dict,
                              extraction_stats: Dict, api_usage: Dict,
                              raw_search_results: List[Dict] = None) -> Dict:
        """Format the final streamlined output structure with optional raw search results."""
        output_data = {
            'metadata': {
                'search_query': search_metadata.get('query', ''),
                'search_terms': search_metadata.get('search_terms', []),
                'total_results': len(profiles),
                'generated_at': datetime.utcnow().isoformat() + 'Z',
                'extraction_version': '2.0',
                'api_calls_used': api_usage.get('api_calls_made', 0)
            },
            'profiles': profiles,
            'extraction_stats': {
                **extraction_stats,
                **self.validation_stats
            }
        }

        # Add raw search results if provided for evaluation and debugging
        if raw_search_results:
            output_data['raw_search_results'] = raw_search_results
            output_data['metadata']['raw_results_count'] = len(raw_search_results)

        return output_data

    def remove_duplicates(self, profiles: List[Dict]) -> List[Dict]:
        """Remove duplicate profiles based on LinkedIn URL."""
        seen_urls = set()
        unique_profiles = []

        for profile in profiles:
            url = profile.get('profile_url')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_profiles.append(profile)

        duplicates_removed = len(profiles) - len(unique_profiles)
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} duplicate profiles")

        return unique_profiles

    def get_validation_stats(self) -> Dict:
        """Return validation statistics."""
        return self.validation_stats.copy()

    def clean_profile_data(self, profile: Dict) -> Dict:
        """Clean and normalize profile data."""
        cleaned = profile.copy()

        # Trim whitespace from string fields
        for field in ['name', 'current_position', 'company', 'location', 'skills', 'summary']:
            if field in cleaned and isinstance(cleaned[field], str):
                cleaned[field] = cleaned[field].strip()

        # Ensure profile URL is properly formatted
        if 'profile_url' in cleaned:
            url = cleaned['profile_url']
            if url and not url.startswith('https://'):
                cleaned['profile_url'] = url.replace('http://', 'https://')

        return cleaned