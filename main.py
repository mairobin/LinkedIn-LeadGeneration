#!/usr/bin/env python3
"""
LinkedIn Lead Generation Data Collection System

A simple Python script that searches Google for LinkedIn profiles and extracts
raw structured data for future AI processing.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
import os  # added
import json  # added

from config.settings import get_settings
from utils.logging_setup import init_logging
import sources  # noqa: F401 ensure registration
from data_validator import DataValidator


 



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

    # Source options
    parser.add_argument(
        '--source', '-s',
        action='append',
        help='Source name to run (repeatable). Default: linkedin_people_google',
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


def _llm_usage_for_run(run_id: str):
    """Aggregate LLM usage from logs/llm_calls.jsonl for the given run_id.

    Returns dict like { 'openai': {'calls': N, 'tokens': T}, 'linkup': {...} }
    """
    result = {}
    try:
        log_path = Path(os.getenv("LLM_LOG_PATH", "logs/llm_calls.jsonl"))
        if not log_path.exists():
            return result
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if not isinstance(rec, dict):
                    continue
                if rec.get("run_id") != run_id:
                    continue
                provider = rec.get("provider") or "unknown"
                usage = rec.get("usage") or {}
                total_tokens = usage.get("total_tokens") or 0
                bucket = result.setdefault(provider, {"calls": 0, "tokens": 0})
                bucket["calls"] += 1
                try:
                    bucket["tokens"] += int(total_tokens)
                except Exception:
                    pass
    except Exception:
        return result
    return result


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
    # LLM usage summary (per provider) for current RUN_ID if tracing enabled
    try:
        run_id = os.getenv("RUN_ID")
        if run_id and os.getenv("LLM_TRACE", "false").lower() in ("1", "true", "yes", "on"):
            usage = _llm_usage_for_run(run_id)
            if usage:
                print("LLM Usage:")
                for provider, stats in usage.items():
                    calls = stats.get('calls', 0)
                    tokens = stats.get('tokens', 0)
                    print(f"  {provider}: calls={calls}, tokens={tokens}")
    except Exception:
        pass
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

        # Ensure a RUN_ID for this process to correlate logs
        try:
            if not os.getenv("RUN_ID"):
                import uuid as _uuid
                os.environ["RUN_ID"] = _uuid.uuid4().hex
        except Exception:
            pass

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

        # Resolve sources
        sources = args.source or ["linkedin_people_google"]
        try:
            from sources.registry import get_source
        except Exception as e:
            logging.error(f"Failed to load sources registry: {e}")
            sys.exit(1)

        all_people = []
        all_companies = []
        api_usage = { 'api_calls_made': 0, 'estimated_daily_limit_used': 'N/A' }
        combined_stats = {}

        for src_name in sources:
            logging.info(f"Running source: {src_name}")
            try:
                src = get_source(src_name)
            except KeyError:
                logging.error(f"Unknown source: {src_name}")
                sys.exit(1)
            data = src.run(search_terms, args.max_results)
            # Attach provenance to each record
            if data:
                for rec in data:
                    try:
                        rec['source_name'] = getattr(src, 'source_name', src_name)
                        rec['source_query'] = ' '.join(search_terms)
                    except Exception:
                        pass
            if getattr(src, 'entity_type', None) == 'person':
                all_people.extend(data or [])
            elif getattr(src, 'entity_type', None) == 'company':
                all_companies.extend(data or [])

        # Prepare output and stats
        validator = DataValidator()
        lookup_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        transformed_profiles = []
        if all_people:
            # Map people to Person schema for output/ingest
            transformed_profiles = map_to_person_schema(all_people, lookup_date)
            # Preserve provenance on mapped profiles
            for src_rec, mapped in zip(all_people, transformed_profiles):
                if isinstance(src_rec, dict) and isinstance(mapped, dict):
                    if 'source_name' in src_rec:
                        mapped['source_name'] = src_rec.get('source_name')
                    if 'source_query' in src_rec:
                        mapped['source_query'] = src_rec.get('source_query')

        output_data = validator.format_output_structure(
            transformed_profiles,
            { 'query': ' '.join(search_terms), 'search_terms': search_terms },
            {},
            api_usage,
            None
        )

        print_summary(output_data, api_usage)

        # Optional DB writes
        if args.write_db:
            try:
                from db.connection import get_connection
                from db import schema as _schema
                from pipelines.ingest_profiles import ingest_profiles
                from pipelines.ingest_companies import ingest_companies

                conn = get_connection(args.db_path)
                _schema.bootstrap(conn)
                processed_people = 0
                processed_companies = 0
                if transformed_profiles:
                    processed_people = ingest_profiles(conn, transformed_profiles)
                if all_companies:
                    # Validate/dedupe/clean companies before ingestion
                    companies_valid = validator.validate_all_companies(all_companies)
                    companies_unique = validator.remove_company_duplicates(companies_valid)
                    companies_cleaned = [validator.clean_company_data(c) for c in companies_unique]
                    processed_companies = ingest_companies(conn, companies_cleaned)
                logging.info(f"DB write complete: people={processed_people}, companies={processed_companies}")
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