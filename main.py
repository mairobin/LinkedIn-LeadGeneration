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
from datetime import datetime
from pathlib import Path

from google_searcher import GoogleSearcher
from data_extractor import LinkedInDataExtractor
from data_validator import DataValidator
from config import DEFAULT_MAX_RESULTS, RAW_OUTPUT_DIR


def setup_logging(log_level: str = "INFO"):
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


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
        default=DEFAULT_MAX_RESULTS,
        help=f'Maximum number of results to collect (default: {DEFAULT_MAX_RESULTS})'
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
        default='INFO',
        help='Set logging level (default: INFO)'
    )

    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Perform a dry run without making API calls'
    )

    parser.add_argument(
        '--use-ai',
        action='store_true',
        help='Use AI-powered extraction for better accuracy (requires OpenAI API key)'
    )

    parser.add_argument(
        '--include-raw-results',
        action='store_true',
        help='Include raw Google search results in output for evaluation and debugging'
    )

    return parser.parse_args()


def ensure_output_directory():
    """Create output directory if it doesn't exist."""
    output_dir = Path(RAW_OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    return output_dir


def generate_output_filename(custom_filename: str = None) -> str:
    """Generate output filename with timestamp."""
    if custom_filename:
        if not custom_filename.endswith('.json'):
            custom_filename += '.json'
        return custom_filename

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f'raw_leads_{timestamp}.json'


def save_results(data: dict, filename: str, output_dir: Path) -> Path:
    """Save results to JSON file."""
    output_path = output_dir / filename

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logging.info(f"Results saved to: {output_path}")
        return output_path

    except Exception as e:
        logging.error(f"Failed to save results: {e}")
        raise


def print_summary(data: dict, api_usage: dict, output_path: Path):
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
    print(f"Output File: {output_path}")
    print("="*60)


def main():
    """Main application entry point."""
    try:
        # Parse arguments
        args = parse_arguments()

        # Setup logging
        setup_logging(args.log_level)

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
            openai_api_key = None
            openai_model = None  # Model is chosen per-operation inside extractor
            if args.use_ai:
                import os
                openai_api_key = os.getenv('OPENAI_API_KEY')
                if not openai_api_key:
                    logging.error("--use-ai specified but OPENAI_API_KEY not found in .env file")
                    sys.exit(1)
                logging.info("Using per-operation model presets (chat: gpt-4o-mini, responses: gpt-4o-mini)")

            extractor = LinkedInDataExtractor(use_ai=args.use_ai, openai_api_key=openai_api_key, openai_model=openai_model)
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
        if args.use_ai:
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

        output_data = validator.format_output_structure(
            cleaned_profiles, search_metadata, combined_stats, api_usage, raw_results
        )

        # Save results
        filename = generate_output_filename(args.output)
        output_path = save_results(output_data, filename, output_dir)

        # Print summary
        print_summary(output_data, api_usage, output_path)

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