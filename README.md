# LinkedIn Lead Generation Tool

A Python tool that searches Google for LinkedIn profiles and extracts structured data for lead generation purposes. The tool uses Google Custom Search API to find LinkedIn profiles and extracts profile information using web scraping with AI extraction (required) and cost‑aware company website discovery.

## Purpose

This project has grown from a basic LinkedIn profile finder into a small data platform for prospecting. It now provides:
- **Profile discovery**: Google Custom Search finds `linkedin.com/in/` profiles via targeted queries.
- **Structured extraction (AI-required)**: Uses OpenAI Chat to extract names, titles, counts, and context.
- **Normalization + storage**: Loads people and companies into a normalized SQLite database with indexes and views.
- **Company enrichment**: Enriches companies via Linkup into a consistent JSON schema (industries, size, website, legal form, etc.).
- **Utilities and reports**: CLI commands to bootstrap the DB, ingest JSON, run enrichment, deduplicate people, and produce reports.
- **Pluggable sources**: A simple source registry makes it easy to add new people/company sources.

## Project Structure

```
.
├── cli.py                     # Unified CLI: run pipelines, bootstrap, enrich, reports, dedupe
# (main.py removed)            # Use cli.py as the single entrypoint
├── google_searcher.py         # Google Custom Search API integration
├── pipelines/
│   ├── runner.py              # Minimal pipeline runner
│   └── steps/
│       ├── extract_data.py    # AI extraction + parsing (moved from data_extractor.py)
│       ├── validate_data.py   # Validation & cleanup (moved from data_validator.py)
│       ├── validate_people.py # People validation step
│       ├── persist_people.py  # Persist people
│       ├── validate_companies.py # Company validation step
│       └── persist_companies.py  # Persist companies
├── sources/                   # Pluggable sources (LinkedIn people, Maps companies scaffold)
├── services/
│   ├── enrichment_service.py  # Linkup enrichment gateway and prompt loading
│   ├── llm_client.py          # Central LLM routing/logging
│   ├── domain_utils.py        # URL/domain and LinkedIn URL normalization helpers
│   ├── mapping.py             # Map raw records to normalized schema
│   └── reporting.py           # Console summaries
├── db/
│   ├── connection.py          # SQLite connection with pragmas
│   ├── schema.py              # Tables, indexes, views; idempotent bootstrap
│   └── repos/
│       ├── people_repo.py     # Person upsert/link operations
│       └── companies_repo.py  # Company upsert/enrichment updates
├── prompts/
│   ├── enrichment_prompt.txt  # Structured enrichment instructions
│   └── profile_extraction_prompt.txt # Profile extraction prompt
├── requirements.txt           # Python dependencies
├── .env.template              # Environment variables template
└── logs/                      # LLM tracing output (when enabled)
```

### File Highlights

- **`cli.py`** - Commands: `bootstrap`, `ingest` (JSON), `run ingest-people`, `run enrich-companies`, `report-*`, `dedupe-people`.
- **`sources/`** - Pluggable sources:
  - `linkedin_people_google` (active)
  - `google_maps_companies` (scaffold; returns empty unless `DEMO=true`)
- **`services/enrichment_service.py`** - Enrichment via Linkup using `prompts/enrichment_prompt.txt`.
- **Normalization** - People persisted and optionally linked to companies; `lookup_date` auto‑set on insert.

## Setup

1. **Install dependencies (Python 3.10+):**
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
   OPENAI_API_KEY=your_openai_api_key  # Required for profile extraction
   LINKUP_API_KEY=your_linkup_api_key  # Optional, required for enrichment
   ```

3. **Set up Google Custom Search:**
   - Create a Google Custom Search Engine at [Google CSE](https://cse.google.com/)
   - Configure it to search the entire web
   - Get your API key from [Google Cloud Console](https://console.cloud.google.com/)

## Usage

### Database quickstart

```bash
# 1) Bootstrap normalized schema (tables, indexes, views)
python cli.py bootstrap

# 2) Run a scrape and write normalized people+companies
python cli.py run ingest-people --query "CEO Berlin" --max-results 5 --write-db --db leads.db

# 3) Verify recent joined rows
python cli.py report-recent --limit 5 --db leads.db

# 4) Spot-check a specific LinkedIn profile (paste URL from recent output)
python cli.py report-person --profile https://linkedin.com/in/example --db leads.db

# 5) Enrichment via unified runner (Linkup only)
AI_PROVIDER=linkup LINKUP_API_KEY=... python cli.py run enrich-companies --db leads.db --limit 20 --progress
```

### Search (AI extraction required)
```bash
# Default source is LinkedIn people via Google CSE
OPENAI_API_KEY=sk-... python cli.py run ingest-people --query "Software Engineer Stuttgart Python" --max-results 50
```

### Multi-Source Examples
```bash
# LinkedIn people (explicit)
python cli.py run ingest-people --source linkedin_people_google --query "CTO Berlin" --write-db

# Add Google Maps companies (scaffold only; returns empty unless DEMO=true)
DEMO=true python cli.py run ingest-people --source google_maps_companies --terms "AI startups" "Berlin" --write-db

# Combine sources in one run
DEMO=true python cli.py run ingest-people --source linkedin_people_google --source google_maps_companies --query "Founder Munich" --write-db
```

### Notes on AI usage
- Profile extraction requires OpenAI Chat; set `OPENAI_API_KEY`.
- Company enrichment is implemented via Linkup only; set `AI_PROVIDER=linkup` and `LINKUP_API_KEY`.
- Website discovery uses domain prediction only (no knowledge/web-search fallback).

### Debug and Testing
```bash
# Run tests (do not touch leads.db; DB tests use tmp files)
pytest -q
```
Set `LOG_LEVEL=DEBUG` to increase verbosity.

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

### Query reporting (new)
- List queries used:
```sql
SELECT id, source, entity_type, query_text, last_executed_at
FROM search_queries
ORDER BY last_executed_at DESC NULLS LAST, created_at DESC;
```
- People totals per query:
```sql
SELECT q.id, q.source, q.query_text, COUNT(p.id) AS people_count
FROM search_queries q
LEFT JOIN people p ON p.search_query_id = q.id
WHERE q.entity_type = 'person'
GROUP BY q.id, q.source, q.query_text
ORDER BY people_count DESC;
```

## Features

### Enrichment and Normalization
- **Company enrichment**: Produces a consistent schema: `Company`, `Legal_Form`, `Industries`, `Locations_Germany`, `Multinational`, `Website`, `Size_Employees`, `Business_Model_Key_Points`, `Products_and_Services`, `Recent_News`.
- **Legal form handling**: Prefers concise suffix from company name (e.g., `GmbH`, `AG`, `GmbH & Co. KG`); normalizes messy values like `GmbH (Germany)` → `GmbH`.
- **People `lookup_date`**: Automatically set at insert to the current UTC date, and backfilled for existing rows.

### Cost-Optimized Website Discovery
- Domain prediction only (free approach). No LLM/web-search fallback.

### CLI Reports and Utilities
- `report-recent`: List recent people with basic company context; filter/sort by connections/followers.
- `report-person`: Print a joined person+company record for a LinkedIn profile.
- `dedupe-people`: Merge duplicates by canonical LinkedIn URL; preserves data and references.

## Configuration

Key settings are centralized in `config/settings.py` (loaded from environment or `.env`). See `.env.template` for all keys.

- **`DEFAULT_MAX_RESULTS`** - Default number of profiles to collect (10)
- **`SEARCH_DELAY`** - Delay between API requests (seconds)
- **`MAX_RETRIES`** - Retries for failed Google calls
- **`LOG_LEVEL`** - Logging level for console output

Environment variables (`.env`):
- `GOOGLE_API_KEY`, `GOOGLE_CSE_ID` — required for search
- `OPENAI_API_KEY` — required for profile extraction
- `LINKUP_API_KEY` — required for Linkup enrichment

## Database Overview

SQLite schema is bootstrapped by `cli.py bootstrap`:
- `people`: normalized person data with unique `linkedin_profile`; `lookup_date` timestamped on insert; includes `search_query_id`.
- `companies`: enrichment targets and attributes (`legal_form`, `industries_json`, `size_employees`, ...); includes `search_query_id`.
- `search_queries`: canonical queries across sources/entity types. One row per normalized query. Pipelines link people/companies to a query.
- `v_people_with_company`: convenient view for joined reads.
- Outreach tables: `outreach_templates`, `outreach_messages` for future messaging workflows.

Run `cli.py report-person --profile <url>` to inspect a joined record, or `cli.py report-recent` to browse recent entries.

## Requirements

- Python 3.10+
- Google Custom Search API key and CSE ID
- OpenAI API key (required for profile extraction)

See `requirements.txt` for complete list of Python dependencies.

## Dataflow

The system follows a mostly linear flow with optional branches for AI enhancement and enrichment. High level:

```text
User Query/Terms (`--query` or `--terms`)
    ↓
Selected source(s) (`--source`): e.g., LinkedIn people, Maps companies (scaffold)
    ↓ results (Google items + pagemap/metatags)
AI-driven extraction (`pipelines.steps.extract_data.LinkedInDataExtractor`)
  - Clean/normalize LinkedIn URLs
  - Extract name/title/location/company from metatags/snippet
  - Use OpenAI to structure fields (position, company, counts)
  - Optional: derive company website/domain (multi-tier, AI-enabled)
    ↓ profiles (raw structured)
Validation & cleanup (`pipelines.steps.validate_data.DataValidator`)
  - validate_all_profiles → remove invalid
  - remove_duplicates → dedupe by canonical URL
  - clean_profile_data → normalize fields
    ↓ cleaned profiles
Optional write to DB (when `--write-db`) via pipelines
  - Resolve canonical query id (`db.repos.queries_repo`) and set `search_query_id` on people/companies
  - Upsert people (`db.repos.people_repo`)
  - Upsert/link companies by domain (`db.repos.companies_repo`) or direct via `pipelines.ingest_companies`
    ↓ SQLite schema (`db/schema.py`)
Optional company enrichment (`cli.py run enrich-companies`)
  - Provider: Linkup only (production). Stub allowed in tests (`RUN_ENV=test`, `AI_PROVIDER=stub`).
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
- Core run (via `cli.py run ingest-people`): search → extract → validate/clean → print; optional DB write at the end.
- Enrichment: separate branch triggered via CLI after ingestion. It does not run during the core search unless you invoke the CLI enrich command.

## Editing Prompts and Models (Simple)

- Models:
  - Set `OPENAI_MODEL` in your environment (defaults to `gpt-4o-mini`).
  - It is consumed via `config/settings.py` as `settings.openai_model` and used by `services/enrichment_service.py` and `pipelines/steps/extract_data.AIProfileExtractor`.

- Prompts:
  - Company enrichment prompt: edit `prompts/enrichment_prompt.txt`.
  - Profile extraction prompt: edit `prompts/profile_extraction_prompt.txt`.
  - Both are loaded at runtime; no code change needed.

- Optional tracing for visibility:
  - Enable `LLM_TRACE=true` (and optionally `LLM_LOG_PATH=logs/llm_calls.jsonl`) to log each LLM call with `provider`, `model`, `prompt_name`, and `prompt_hash`.

### Per-use-case LLM routing

- Central routing config: `config/llm_routes.py` defines defaults per use-case:
  - `profile_extraction` (chat via OpenAI)
  - `company_enrichment` (Linkup)
- Provider for `company_enrichment` is Linkup only (OpenAI path not implemented in code).
- Global default model: `OPENAI_MODEL`.
- Per-use-case overrides via env vars (optional):
  - `OPENAI_MODEL_ENRICHMENT`, `OPENAI_MODEL_PROFILE`
  - You can also switch providers for a route with `LLM_*_PROVIDER` (currently only `openai` implemented).
- To use Linkup for enrichment, set `LLM_ENRICHMENT_PROVIDER=linkup` and provide `LINKUP_API_KEY`. The code will route `company_enrichment` to Linkup automatically.
- Code calls go through `services/llm_client.LLMClient`, so you only edit `config/llm_routes.py` or env vars to change models/params for a specific step.
  
## LLM Usage 

- **Central gateway**: All LLM calls go through `services/llm_client.LLMClient`.
  - `chat` → OpenAI Chat Completions (used for profile extraction)
  - `enrich_company` → Linkup structured research (used for company enrichment)

- **Routing per use‑case**: `config/llm_routes.py` defines defaults.
  - `profile_extraction` → provider `openai` by default; model from `OPENAI_MODEL_PROFILE` or global `OPENAI_MODEL`.
  - `company_enrichment` → provider `linkup` only.

- **Env overrides (no code change)**:
  - Providers: `LLM_PROFILE_PROVIDER` (OpenAI only supported for extraction)
  - Models: `OPENAI_MODEL_PROFILE` (fallback `OPENAI_MODEL`)

- **Prompts loaded at runtime**:
  - Profile extraction: `prompts/profile_extraction_prompt.txt`
  - Company enrichment: `prompts/enrichment_prompt.txt`

- **Where LLM is used in pipelines**:
  - People ingestion: OpenAI extraction in `pipelines/steps/extract_data.py`
  - Company enrichment: Linkup via `services/enrichment_service.py`

- **Settings & auth**: `config/settings.py` controls `AI_ENABLED`, reads keys.
  - OpenAI: `OPENAI_API_KEY`
  - Linkup: `LINKUP_API_KEY`

- **Tracing & observability**: Set `LLM_TRACE=true` (optional `LLM_LOG_PATH`).
  - Logs to `logs/llm_calls.jsonl` with provider, model, operation, prompt name, and prompt hash.

- **Not used**: embeddings.

- **Structured output**: JSON‑only prompts and Pydantic validation (enrichment returns structured output).
