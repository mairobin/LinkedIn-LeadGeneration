# LinkedIn Lead Generation Tool

A Python tool that searches Google for LinkedIn profiles and extracts structured data for lead generation purposes. The tool uses Google Custom Search API to find LinkedIn profiles and extracts profile information using web scraping with optional AI enhancement and automatic company website discovery.

## Purpose

This tool helps businesses and recruiters collect LinkedIn profile data at scale by:
- Searching for LinkedIn profiles using specific criteria (job titles, locations, skills)
- Extracting structured profile information automatically
- Finding company websites with cost-optimized AI search
- Validating and cleaning extracted data
- Exporting results in JSON format for further processing

## Project Structure

```
.
├── main.py              # Main entry point and CLI interface
├── google_searcher.py   # Google Custom Search API integration
├── data_extractor.py    # LinkedIn profile data extraction logic
├── data_validator.py    # Data validation and cleaning utilities
├── config.py           # Configuration settings and constants
├── requirements.txt    # Python dependencies
├── .env.template       # Environment variables template
└── output/            # Generated JSON files directory
```

### File Descriptions

- **`main.py`** - Command-line interface and orchestration logic
- **`google_searcher.py`** - Handles Google Custom Search API requests to find LinkedIn profiles
- **`data_extractor.py`** - Scrapes LinkedIn profile pages, extracts structured data, and finds company websites
- **`data_validator.py`** - Validates extracted data, removes duplicates, and ensures data quality
- **`config.py`** - Contains all configuration settings, API keys, and validation rules

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment variables:**
   ```bash
   cp .env.template .env
   ```
   Edit `.env` and add your API keys:
   ```
   GOOGLE_API_KEY=your_google_custom_search_api_key
   GOOGLE_CSE_ID=your_custom_search_engine_id
   OPENAI_API_KEY=your_openai_api_key  # Optional, for AI-powered extraction
   ```

3. **Set up Google Custom Search:**
   - Create a Google Custom Search Engine at [Google CSE](https://cse.google.com/)
   - Configure it to search the entire web
   - Get your API key from [Google Cloud Console](https://console.cloud.google.com/)

## Usage

### Basic Search (No AI - Default)
```bash
python main.py --query "Software Engineer Stuttgart Python" --max-results 50
```

### AI-Enhanced Search with Website Discovery
```bash
python main.py --terms "Data Scientist" "Berlin" "Machine Learning" --use-ai --max-results 30
```

**Note:** By default, the tool runs without AI to minimize costs. Add `--use-ai` to enable:
- Enhanced profile data extraction 
- Automatic company website discovery
- Better data accuracy (requires OpenAI API key)

### Debug and Testing
```bash
# Dry run (no API calls)
python main.py --dry-run --query "Product Manager"

# Include raw search results for debugging
python main.py --query "CEO" --include-raw-results --max-results 10

# Verbose logging
python main.py --query "Engineer" --log-level DEBUG
```

## Output Format

Results are saved as JSON files in the `output/` directory:

```json
{
  "profiles": [
    {
      "name": "John Doe",
      "profile_url": "https://linkedin.com/in/johndoe",
      "current_position": "Senior Software Engineer",
      "company": "Tech Corp",
      "company_website": "https://techcorp.com",
      "location": "Berlin, Germany",
      "summary": "Experienced software engineer specializing in...",
      "follower_count": "1,234",
      "connection_count": "500+",
      "email": "john.doe@example.com",
      "website": "https://johndoe.dev",
      "phone": "+49 123 4567890",
      "experience_years": 10,
      "summary_other": [
        "Scaled platform to 3M MAU",
        "Open-source contributor at AwesomeProject"
      ]
    }
  ],
  "metadata": {
    "search_query": "site:linkedin.com/in/ Software Engineer Berlin",
    "search_terms": ["Software", "Engineer", "Berlin"],
    "total_results": 25,
    "api_calls_used": 3,
    "generated_at": "2024-09-22T10:30:00Z"
  },
  "extraction_stats": {
    "successful_extractions": 23,
    "failed_extractions": 2,
    "duplicate_profiles_removed": 1,
    "valid_profiles": 22,
    "invalid_profiles": 1
  },
  "api_usage": {
    "estimated_daily_limit_used": "0.3%"
  }
}
```

## Features

### Cost-Optimized Website Discovery
**Only available with `--use-ai` flag.** The tool automatically finds company websites using a three-tier approach:

1. **Domain prediction** (free) - Tests common domain patterns like company.com, company.de
2. **Knowledge-based AI** (cheap) - Uses AI's training data for well-known companies  
3. **Web search AI** (expensive) - Last resort, searches the web for complex cases

This approach reduces API costs by ~95% while maintaining complete coverage.

## Configuration

Key settings can be modified in `config.py`:

- **`DEFAULT_MAX_RESULTS`** - Default number of profiles to collect (10)
- **`SEARCH_DELAY`** - Delay between API requests in seconds (1)
- **`RAW_OUTPUT_DIR`** - Output directory for JSON files ("output")
- **`REQUIRED_FIELDS`** - Fields that must be present in valid profiles
- **`MAX_RETRIES`** - Number of retry attempts for failed requests (3)

## Requirements

- Python 3.7+
- Google Custom Search API key and CSE ID
- OpenAI API key (optional, for enhanced extraction accuracy)

See `requirements.txt` for complete list of Python dependencies.