#!/usr/bin/env python3
"""
LinkedIn Lead Generation Data Collection System

A simple Python script that searches Google for LinkedIn profiles and extracts
raw structured data for future AI processing.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from google_searcher import GoogleSearcher
from data_extractor import LinkedInDataExtractor
from data_validator import DataValidator
from config.settings import get_settings
from utils.logging_setup import init_logging


 


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='LinkedIn Lead Generation Data Collection System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --query "Software Engineer Stuttgart Python" --max-results 50
  python main.py --terms "Data Scientist" "Berlin" "Machine Learning" --max-results 30
  python main.py --query "Product Manager" --max-results 20 --output custom_leads.json
        """
    )

    # Query input options (mutually exclusive)
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument(
        '--query', '-q',
        type=str,
        help='Search query as a single string (e.g., "Software Engineer Stuttgart Python")'
    )
    query_group.add_argument(
        '--terms', '-t',
        nargs='+',
        help='Search terms as separate arguments (e.g., --terms "Software Engineer" "Stuttgart" "Python")'
    )

    # Output options
    parser.add_argument(
        '--max-results', '-m',
        type=int,
        default=get_settings().default_max_results,
        help='Maximum number of results to collect'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Custom output filename (default: raw_leads_YYYYMMDD_HHMMSS.json)'
    )

    # Debug options
    parser.add_argument(
        '--log-level', '-l',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=get_settings().log_level,
        help='Set logging level (default: from settings)'
    )

    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Perform a dry run without making API calls'
    )


    parser.add_argument(
        '--include-raw-results',
        action='store_true',
        help='Include raw Google search results in output for evaluation and debugging'
    )

    # Database options
    parser.add_argument(
        '--write-db',
        action='store_true',
        help='Write normalized people/companies to SQLite (default: off)'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=get_settings().db_path,
        help='Path to SQLite database file (default: from settings)'
    )

    

    return parser.parse_args()


def _parse_int_shorthand(value):
    """Parse strings like '1.2K', '3M', '4500', '500+' into an integer."""
    if value is None:
        return None
    try:
        s = str(value).strip().upper()
        if not s:
            return None
        if s.endswith('+'):
            s = s[:-1]
        import re as _re
        m = _re.match(r"^([0-9]+(?:\.[0-9]+)?)([KMB]?)$", s)
        if m:
            num = float(m.group(1))
            suf = m.group(2)
            factor = 1
            if suf == 'K':
                factor = 1000
            elif suf == 'M':
                factor = 1000000
            elif suf == 'B':
                factor = 1000000000
            return int(round(num * factor))
        digits = ''.join(ch for ch in s if ch.isdigit())
        return int(digits) if digits else None
    except Exception:
        return None


def _parse_connections(value):
    parsed = _parse_int_shorthand(value)
    if parsed is None:
        return None
    return min(parsed, 500)


def map_to_person_schema(profiles, lookup_date):
    """Map extracted profiles to the Person schema (inline, simple)."""
    mapped = []
    for p in profiles:
        mapped.append({
            'Contact_Name': p.get('name') or '',
            'LinkedIn_Profile': p.get('profile_url') or None,
            'Company': p.get('company') or None,
            'Company_Website': None,
            'Company_Domain': p.get('company_domain') or None,
            'Location': p.get('location') or None,
            'Position': p.get('current_position') or None,
            'Connections_LinkedIn': _parse_connections(p.get('connection_count')) if p.get('connection_count') is not None else None,
            'Followers_LinkedIn': _parse_int_shorthand(p.get('follower_count')) if p.get('follower_count') is not None else None,
            'Website_Info': None,
            'Phone_Info': p.get('phone') or '',
            'Info_raw': p.get('summary') or '',
            'Insights': p.get('summary_other') if isinstance(p.get('summary_other'), list) else [],
            'Email': p.get('email') or None,
            'Lookup_Date': lookup_date or None,
            'Hot': False,
            'Last_Interaction_Date': None,
            'Status': None,
            'Notes': [],
        })
    return mapped


def ensure_output_directory():
    """(Deprecated) No-op: JSON file creation removed in favor of console output."""
    return Path(get_settings().raw_output_dir)


def generate_output_filename(custom_filename: str = None) -> str:
    """(Deprecated) Kept for backwards-compatibility; files are no longer written."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return custom_filename or f'raw_leads_{timestamp}.json'


def save_results(data: dict, filename: str, output_dir: Path) -> Path:
    """(Deprecated) Disabled: no file writes. Returns a dummy path."""
    return output_dir / filename


def print_summary(data: dict, api_usage: dict, output_path: Path = None):
    """Print summary of the extraction process."""
    metadata = data.get('metadata', {})
    extraction_stats = data.get('extraction_stats', {})

    print("\n" + "="*60)
    print("LINKEDIN LEAD GENERATION - SUMMARY")
    print("="*60)
    print(f"Search Query: {metadata.get('search_query', 'N/A')}")
    print(f"Total Results: {metadata.get('total_results', 0)}")
    print(f"API Calls Made: {metadata.get('api_calls_used', 0)}")
    print(f"Generated At: {metadata.get('generated_at', 'N/A')}")
    print()
    print("Extraction Statistics:")
    print(f"  Successful Extractions: {extraction_stats.get('successful_extractions', 0)}")
    print(f"  Failed Extractions: {extraction_stats.get('failed_extractions', 0)}")
    print(f"  Duplicates Removed: {extraction_stats.get('duplicate_profiles_removed', 0)}")
    print(f"  Valid Profiles: {extraction_stats.get('valid_profiles', 0)}")
    print(f"  Invalid Profiles: {extraction_stats.get('invalid_profiles', 0)}")
    print()
    print(f"API Usage: {api_usage.get('estimated_daily_limit_used', 'N/A')}")
    if output_path:
        print(f"Output File: {output_path}")
    print("="*60)


def main():
    """Main application entry point."""
    try:
        # Parse arguments
        args = parse_arguments()

        # Setup logging (idempotent)
        init_logging(args.log_level)

        # Determine search terms
        if args.query:
            search_terms = args.query.split()
        else:
            search_terms = args.terms

        logging.info("Starting LinkedIn Lead Generation Data Collection")
        logging.info(f"Search terms: {search_terms}")
        logging.info(f"Max results: {args.max_results}")

        if args.dry_run:
            logging.info("DRY RUN MODE - No API calls will be made")
            print("Dry run completed. Search terms:", search_terms)
            return

        # Ensure output directory exists
        output_dir = ensure_output_directory()

        # Initialize components
        try:
            searcher = GoogleSearcher()

            # Initialize extractor with AI option
            settings = get_settings()
            openai_api_key = settings.openai_api_key
            use_ai = settings.ai_enabled
            extractor = LinkedInDataExtractor(
                use_ai=use_ai,
                openai_api_key=openai_api_key,
                openai_model=None,
            )
            validator = DataValidator()
        except ValueError as e:
            logging.error(f"Configuration error: {e}")
            logging.error("Please check your .env file and ensure API keys are set")
            sys.exit(1)

        # Execute search
        logging.info("Starting Google search...")
        search_results = searcher.search_linkedin_profiles(search_terms, args.max_results)

        if not search_results:
            logging.error("No search results returned")
            sys.exit(1)

        # Extract profile data
        logging.info("Extracting profile data...")
        profiles = extractor.extract_all_profiles(search_results)

        if not profiles:
            logging.error("No profiles extracted successfully")
            sys.exit(1)

        # Validate and clean data
        logging.info("Validating profile data...")
        valid_profiles = validator.validate_all_profiles(profiles)

        # Remove duplicates
        unique_profiles = validator.remove_duplicates(valid_profiles)

        # Clean profile data
        cleaned_profiles = [validator.clean_profile_data(profile) for profile in unique_profiles]

        # Enhance with website information if AI is enabled
        if settings.ai_enabled:
            logging.info("Enhancing profiles with company websites...")
            cleaned_profiles = extractor.enhance_profiles_with_websites(cleaned_profiles)

        # Get statistics
        extraction_stats = extractor.get_extraction_stats()
        validation_stats = validator.get_validation_stats()
        api_usage = searcher.get_api_usage()

        # Combine stats
        combined_stats = {**extraction_stats, **validation_stats}

        # Format output
        search_metadata = {
            'query': searcher.format_linkedin_query(search_terms),
            'search_terms': search_terms
        }

        # Include raw search results if requested
        raw_results = search_results if args.include_raw_results else None

        # Transform profiles to new Person schema for output
        lookup_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        transformed_profiles = map_to_person_schema(cleaned_profiles, lookup_date)

        output_data = validator.format_output_structure(
            transformed_profiles, search_metadata, combined_stats, api_usage, raw_results
        )

        # Print summary to console (no JSON file written)
        print_summary(output_data, api_usage)

        # Optional: write normalized tables (people + companies) and link by domain
        if args.write_db:
            try:
                from db.connection import get_connection
                from db import schema as _schema
                from pipelines.ingest_profiles import ingest_profiles

                conn = get_connection(args.db_path)
                _schema.bootstrap(conn)
                # Use the already transformed person schema for ingestion
                processed = ingest_profiles(conn, transformed_profiles)
                logging.info(f"Normalized DB write complete: {processed} profiles processed → people/companies")
                # Print a concise preview of top 5 leads
                preview = output_data.get('profiles', [])[:5]
                if preview:
                    print("Top 5 leads:")
                    for p in preview:
                        print(f"- {p.get('Contact_Name') or p.get('name')} | {p.get('LinkedIn_Profile') or p.get('profile_url')} | {p.get('Position') or p.get('current_position')} | {p.get('Company') or p.get('company')}")
            except Exception as _e:
                logging.error(f"DB write failed: {_e}")

        

        logging.info("Data collection completed successfully")

    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        logging.debug("Full traceback:", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()