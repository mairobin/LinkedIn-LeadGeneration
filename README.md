# LinkedIn Lead Generation Tool

A Python tool that searches Google for LinkedIn profiles and extracts structured data for lead generation purposes. The tool uses Google Custom Search API to find LinkedIn profiles and extracts profile information using web scraping with optional AI enhancement and automatic company website discovery.

## Purpose

This project has grown from a basic LinkedIn profile finder into a small data platform for prospecting. It now provides:
- **Profile discovery**: Google Custom Search finds `linkedin.com/in/` profiles via targeted queries.
- **Structured extraction**: Scrapes snippets and optionally uses AI to extract names, titles, counts, and context.
- **Normalization + storage**: Loads people and companies into a normalized SQLite database with indexes and views.
- **Company enrichment**: Enriches companies via OpenAI or Linkup into a consistent JSON schema (industries, size, website, legal form, etc.).
- **Utilities and reports**: CLI commands to bootstrap the DB, ingest JSON, run enrichment, deduplicate people, and produce reports.
- **Cost-aware design**: AI is optional; default flows are non-AI to minimize cost while keeping quality.

## Project Structure

```
.
├── cli.py                     # DB-focused CLI: bootstrap, ingest, enrich, reports, dedupe
├── main.py                    # Search + extraction runner (JSON output; optional AI)
├── google_searcher.py         # Google Custom Search API integration
├── data_extractor.py          # Extraction and heuristic parsing from snippets/meta
├── data_validator.py          # Validation utilities (dedupe/quality)
├── services/
│   ├── enrichment_service.py  # OpenAI/Linkup enrichment, prompt loading
│   └── domain_utils.py        # URL/domain and LinkedIn URL normalization helpers
├── pipelines/
│   ├── ingest_profiles.py     # Normalize and upsert people + link to companies
│   └── enrich_companies.py    # Batch enrichment and persistence
├── db/
│   ├── connection.py          # SQLite connection with pragmas
│   ├── schema.py              # Tables, indexes, views; idempotent bootstrap
│   └── repos/
│       ├── people_repo.py     # Person upsert/link operations
│       └── companies_repo.py  # Company upsert/enrichment updates
├── prompts/
│   └── enrichment_prompt.txt  # Structured enrichment instructions
├── sqlite_storage.py          # Snapshot table storage utility
├── requirements.txt           # Python dependencies
├── .env.template              # Environment variables template
└── output/                    # Legacy/optional output directory (no files by default)
```

### File Highlights

- **`cli.py`** - Database-centric commands (bootstrap, ingest JSON, enrich, reports, dedupe)
- **`main.py`** - Search/extract runner; writes JSON (no DB unless used via CLI ingest)
- **`services/enrichment_service.py`** - Calls OpenAI/Linkup using `prompts/enrichment_prompt.txt`
- **`pipelines/ingest_profiles.py`** - Normalizes person/company and sets `lookup_date` automatically
- **`pipelines/enrich_companies.py`** - Saves enrichment and normalizes legal forms

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

### Database quickstart (must-have flow)

```bash
# 1) Bootstrap normalized schema (tables, indexes, views)
python cli.py bootstrap

# 2) Run a scrape and write normalized people+companies
python main.py --query "CEO Berlin" --max-results 5 --write-db --db-path leads.db

# 3) Verify recent joined rows
python cli.py report-recent --limit 5 --db leads.db

# 4) Spot-check a specific LinkedIn profile (paste URL from recent output)
python cli.py report-person --profile https://linkedin.com/in/example --db leads.db

# 5) Enrichment: stub, OpenAI, or Linkup
# Stub (no network):
python cli.py enrich --db leads.db --limit 20

# Enrichment provider is selected via settings.ai_provider (stub|openai|linkup)
# OpenAI (requires OPENAI_API_KEY; AI_PROVIDER=openai):
AI_PROVIDER=openai python cli.py enrich --db leads.db --limit 20 --progress

# Linkup (requires LINKUP_API_KEY; AI_PROVIDER=linkup):
AI_PROVIDER=linkup python cli.py enrich --db leads.db --limit 20 --progress
```

### Basic Search (No AI - Default)
```bash
python main.py --query "Software Engineer Stuttgart Python" --max-results 50
```

### AI-Enhanced Search with Website Discovery
```bash
AI_ENABLED=true OPENAI_API_KEY=sk-... python main.py --terms "Data Scientist" "Berlin" "Machine Learning" --max-results 30
```

AI usage is controlled by settings: set `AI_ENABLED=true` for the search/extraction flow, and select enrichment provider via `AI_PROVIDER`.

### Debug and Testing
```bash
# Dry run (no API calls)
python main.py --dry-run --query "Product Manager"

# Include raw search results for debugging
python main.py --query "CEO" --include-raw-results --max-results 10

# Verbose logging
python main.py --query "Engineer" --log-level DEBUG
```

## Output

By default the tool prints a human-readable summary to the console (no files are written). Example:

```
============================================================
LINKEDIN LEAD GENERATION - SUMMARY
============================================================
Search Query: site:linkedin.com/in/ "Software Engineer" Berlin
Total Results: 25
API Calls Made: 3
Generated At: 2025-09-25T10:30:00Z

Extraction Statistics:
  Successful Extractions: 23
  Failed Extractions: 2
  Duplicates Removed: 1
  Valid Profiles: 22
  Invalid Profiles: 1

API Usage: 3
============================================================
```

If you pass `--write-db`, normalized data is written to SQLite. Use CLI reports to inspect data:

- Recent list:
```bash
python cli.py report-recent --limit 5 --db leads.db
```

- Single person (joined with company):
```bash
python cli.py report-person --profile https://linkedin.com/in/example --db leads.db
```

## Features

### Enrichment and Normalization
- **Company enrichment**: Produces a consistent schema: `Company`, `Legal_Form`, `Industries`, `Locations_Germany`, `Multinational`, `Website`, `Size_Employees`, `Business_Model_Key_Points`, `Products_and_Services`, `Recent_News`.
- **Legal form handling**: Prefers concise suffix from company name (e.g., `GmbH`, `AG`, `GmbH & Co. KG`); normalizes messy values like `GmbH (Germany)` → `GmbH`.
- **People `lookup_date`**: Automatically set at insert to the current UTC date, and backfilled for existing rows.

### Cost-Optimized Website Discovery
When AI is enabled, website discovery follows a staged approach:
1. Domain prediction (free)
2. Knowledge-based AI (cheap)
3. Web search AI (fallback)

This reduces API cost while keeping coverage.

### CLI Reports and Utilities
- `report-recent`: List recent people with basic company context; filter/sort by connections/followers.
- `report-person`: Print a joined person+company record for a LinkedIn profile.
- `dedupe-people`: Merge duplicates by canonical LinkedIn URL; preserves data and references.

## Configuration

Key settings are centralized in `config/settings.py` (loaded from environment or `.env`). See `.env.template` for all keys.

- **`DEFAULT_MAX_RESULTS`** - Default number of profiles to collect (10)
- **`SEARCH_DELAY`** - Delay between API requests (seconds)
- **`RAW_OUTPUT_DIR`** - Output directory for JSON files ("output")
- **`MAX_RETRIES`** - Retries for failed Google calls

Environment variables (`.env`):
- `GOOGLE_API_KEY`, `GOOGLE_CSE_ID` — required for search
- `OPENAI_API_KEY` — optional, enables OpenAI enrichment
- `LINKUP_API_KEY` — optional, enables Linkup enrichment

## Database Overview

SQLite schema is bootstrapped by `cli.py bootstrap`:
- `people`: normalized person data with unique `linkedin_profile`; `lookup_date` timestamped on insert.
- `companies`: enrichment targets and attributes (`legal_form`, `industries_json`, `size_employees`, ...).
- `v_people_with_company`: convenient view for joined reads.
- Outreach tables: `outreach_templates`, `outreach_messages` for future messaging workflows.

Run `cli.py report-person --profile <url>` to inspect a joined record, or `cli.py report-recent` to browse recent entries.

## Requirements

- Python 3.7+
- Google Custom Search API key and CSE ID
- OpenAI API key (optional, for enhanced extraction accuracy)

See `requirements.txt` for complete list of Python dependencies.

## Dataflow

The system follows a mostly linear flow with optional branches for AI enhancement and enrichment. High level:

```text
User Query/Terms
    ↓
Google CSE search (`google_searcher.GoogleSearcher`)
    ↓ results (Google items + pagemap/metatags)
AI-driven extraction (`data_extractor.LinkedInDataExtractor`)
  - Clean/normalize LinkedIn URLs
  - Extract name/title/location/company from metatags/snippet
  - Use OpenAI to structure fields (position, company, counts)
  - Optional: derive company website/domain (multi-tier, AI-enabled)
    ↓ profiles (raw structured)
Validation & cleanup (`data_validator.DataValidator`)
  - validate_all_profiles → remove invalid
  - remove_duplicates → dedupe by canonical URL
  - clean_profile_data → normalize fields
    ↓ cleaned profiles
Optional write to DB (when `--write-db`) via `pipelines.ingest_profiles`
  - Upsert people (`db.repos.people_repo`)
  - Upsert/link companies by domain (`db.repos.companies_repo`)
    ↓ SQLite schema (`db/schema.py`)
Optional company enrichment (`cli.py enrich` → `pipelines.enrich_companies`)
  - Provider: stub | OpenAI | Linkup (`services/enrichment_service`)
  - Update company attributes (industries, size, website, legal form, news)
```

### Step-by-step (no-code journey)
- Search: We query Google Custom Search for `site:linkedin.com/in` profiles based on your terms.
- Extract: For each result, we parse metatags/snippets and use an LLM to structure profile fields. If AI is enabled, we also attempt company website/domain discovery cost‑effectively.
- Validate: We validate required fields, remove obvious duplicates, and normalize text/URLs.
- Output: By default, results are printed as a summary; optionally, we transform to a normalized Person schema.
- Persist (optional): If `--write-db` is used, we upsert People and Companies into SQLite and link them by domain.
- Enrich (optional branch): Separately, you can run `cli.py enrich` to fill company attributes (industries, size, legal form, website, news). This step reads pending companies from the DB and writes back enrichment.

### Linear vs branches
- Core run (`main.py`): linear search → extract → validate/clean → print; with optional AI website discovery inside extraction, and optional DB write at the end.
- Enrichment: separate branch triggered via CLI after ingestion. It does not run during the core search unless you invoke the CLI enrich command.