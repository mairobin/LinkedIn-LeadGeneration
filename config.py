"""
Configuration settings for LinkedIn lead generation system.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Settings
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.getenv('GOOGLE_CSE_ID')
GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"

# Rate limiting and retry settings
SEARCH_DELAY = 1  # seconds between requests
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30  # seconds

# Output Settings
RAW_OUTPUT_DIR = "output"
PROCESSED_OUTPUT_DIR = "processed"  # For future AI processing

# Data Validation
REQUIRED_FIELDS = ["name", "profile_url", "summary"]
OPTIONAL_FIELDS = [
    "current_position",
    "company",
    "location",
    "follower_count",
    "connection_count",
    # Summary-derived optional fields
    "email",
    "website",
    "phone",
    "experience_years",
    "summary_other",
]

# Search Settings
DEFAULT_MAX_RESULTS = 10
RESULTS_PER_PAGE = 10  # Google Custom Search API limit

# Data extraction version for tracking
EXTRACTION_VERSION = "1.0"

# LinkedIn URL patterns
LINKEDIN_URL_PATTERNS = [
    "linkedin.com/in/",
    "de.linkedin.com/in/",
    "www.linkedin.com/in/"
]

# Logging
LOG_LEVEL = "INFO"