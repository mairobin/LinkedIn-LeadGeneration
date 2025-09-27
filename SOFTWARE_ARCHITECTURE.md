## Software Architecture

### Purpose
Provide an accurate, visual-first description of the system so newcomers can understand key flows, change the code safely, and operate it locally and in CI.

### Scope and Audience
- For developers and stakeholders of the LinkedIn Lead Generation CLI and pipelines.
- Out-of-scope: browser automation and LinkedIn scraping; production cloud deployment.

## Context (C4 Level 1)

```mermaid
flowchart LR
  user["User / CLI"] --> cli["CLI: python cli.py"]
  cli --> google[Google CSE]
  cli --> openai[OpenAI Chat]
  cli --> linkup[Linkup]
  cli --> sqlite[(SQLite DB)]
```

Takeaway: the CLI orchestrates Google Search, AI extraction, optional Linkup enrichment, and writes to SQLite.

## Containers (C4 Level 2)

```mermaid
flowchart TB
  subgraph CLI
    A[cli.py]
  end
  subgraph Pipelines
    R[runner.py]
    S1[steps: extract/validate]
    S2[steps: persist]
    S3[steps: enrichment]
  end
  subgraph Sources
    L[linkedin_people]
    LG[linkedin_companies_google]
    GM[google_maps_companies]
  end
  subgraph Services
    E[enrichment_service]
    LC[llm_client]
    DU[domain_utils]
    M[mapping]
    REP[reporting]
  end
  subgraph DB
    SCH[schema]
    PR[people_repo]
    CR[companies_repo]
    OR[outreach_repo]
  end

  A --> R
  R --> L
  R --> LG
  R --> GM
  L --> S1
  S1 --> S2
  S3 --> CR
  S2 --> PR
  S2 --> CR
  A --> S3
  S3 --> LC
  LC --> E
  E --> linkup
  R --> REP
  SCH --> PR
  SCH --> CR
  SCH --> OR
```

Key files: `cli.py`, `pipelines/runner.py`, `pipelines/steps/*`, `sources/*`, `services/*`, `db/*`, `config/*`.

## Components (C4 Level 3)

### People Ingestion runtime
```mermaid
sequenceDiagram
  participant U as User
  participant C as cli.py
  participant Src as LinkedInPeopleSource
  participant GS as GoogleSearcher
  participant EX as LinkedInDataExtractor
  participant DV as DataValidator
  participant DB as SQLite

  U->>C: run ingest-people -q "..." --write-db
  C->>Src: run(terms, max_results)
  Src->>GS: search_linkedin_profiles
  GS-->>Src: Google results
  Src->>EX: extract_all_profiles (OpenAI required)
  EX-->>Src: profiles
  Src->>DV: validate_all_profiles / remove_duplicates
  DV-->>Src: cleaned profiles
  C->>DB: PersistPeople (resolve canonical search_query_id)
  DB-->>C: ids
  C-->>U: summary + counts
```

### Company Enrichment runtime
```mermaid
sequenceDiagram
  participant C as cli.py (run enrich-companies)
  participant ER as enrichment_service
  participant LC as llm_client
  participant LU as Linkup
  participant DB as SQLite

  C->>DB: LoadPendingCompanies
  C->>ER: fetch_company_enrichment(name, domain)
  ER->>LC: enrich_company(...)
  LC->>LU: search(structured)
  LU-->>LC: structured JSON
  LC-->>ER: normalized dict
  ER-->>C: enrichment fields
  C->>DB: update_enrichment + last_enriched_at
```

## Data Model and Lifecycle

Tables: `people`, `companies`, `search_queries`; view `v_people_with_company`; outreach tables `outreach_templates`, `outreach_messages`.

Invariants: `people.linkedin_profile` UNIQUE and normalized; `companies.domain` UNIQUE when known.

```mermaid
erDiagram
  SEARCH_QUERIES ||--o{ PEOPLE : produced
  SEARCH_QUERIES ||--o{ COMPANIES : produced
  COMPANIES ||--o{ PEOPLE : has
  PEOPLE {
    int id
    string linkedin_profile
    string first_name
    string last_name
    string title_current
    string email
    string location_text
    int connections_linkedin
    int followers_linkedin
    string lookup_date
    int company_id
    int search_query_id
    string source_name
    string source_query
  }
  COMPANIES {
    int id
    string name
    string domain
    string website
    string legal_form
    string industries_json
    string locations_de_json
    int size_employees
    string last_enriched_at
    int search_query_id
    string source_name
    string source_query
  }
  SEARCH_QUERIES {
    int id
    string source
    string entity_type
    string query_text
    string normalized_query
    string created_at
    string last_executed_at
  }
  OUTREACH_TEMPLATES {
    int id
    string name
    string channel
    string body_md
    string variables_json
    int is_active
    string created_at
  }
  OUTREACH_MESSAGES {
    int id
    string linkedin_profile
    string channel
    int template_id
    int stage_no
    string rendered_md
    string status
    string scheduled_at
    string sent_at
    string replied_at
    string created_at
  }
```

## Responsibilities and Constraints
- Extraction requires OpenAI. `pipelines/steps/extract_data.py` enforces AI usage and raises when `OPENAI_API_KEY` is missing.
- Enrichment defaults to Linkup (`config/llm_routes.py`) and needs `LINKUP_API_KEY`.
- Persistence is idempotent by `linkedin_profile` and `domain`. Normalization via `services/domain_utils.normalize_linkedin_profile_url` and `extract_apex_domain`.
- Canonical query linkage: pipelines find-or-create `search_queries` and set `people.search_query_id` (and for companies as pipelines are added).

## Deployment and Operations
- Config via `.env` loaded by `config/settings.py`:
  - AI: `AI_ENABLED`, `AI_PROVIDER` (stub|openai|linkup), `OPENAI_MODEL`, `OPENAI_API_KEY`, `LINKUP_API_KEY`.
  - Google: `GOOGLE_API_KEY`, `GOOGLE_CSE_ID`, `GOOGLE_SEARCH_URL`, rate/timeout limits.
  - Runtime: `DB_PATH`, `RUN_ENV`, `LOG_LEVEL`, `DEMO`.
  - Tracing: `LLM_TRACE`, `LLM_LOG_PATH`.
- Observability: LLM calls logged to `logs/llm_calls.jsonl` when tracing enabled. `RUN_ID` set in `cli.py` and propagated to logs.
- Failure modes: Google quota, OpenAI failures, Linkup unavailability, DB locks. Prefer fail-fast with clear messages and continue-on-error where safe in persistence.

### Data Retention (brief)
- Keep personal profile data only as long as needed for outreach experiments.
- If a profile is no longer needed, remove it (e.g., via a simple SQL DELETE on `people` and related outreach rows). A fuller deletion/export tool can be added if required later.

## Decisions (ADR summaries)
- AI extraction is required for people ingestion (accuracy over cost).
- Enrichment provider is Linkup by default; OpenAI enrichment path removed in `services/llm_client.py`.
- Website discovery for enrichment uses domain prediction + Linkup; no general web-scraping in core.
- Query tracking uses a canonical `search_queries` table with FKs in `people` (and later `companies`), keeping the design KISS-ready with optional future upgrades (run history, M:N attribution).

## Commands (Reproduce Flows)
- Bootstrap DB: `python cli.py bootstrap`
- Ingest people (no DB): `python cli.py run ingest-people -q "Senior Engineer Berlin" -m 5`
- Ingest people (DB write): `python cli.py run ingest-people -q "Senior Engineer Berlin" -m 5 --write-db`
- Enrich companies: `python cli.py run enrich-companies --limit 5 --progress`
- Reports: `python cli.py report-recent --limit 5` | `python cli.py report-person --profile https://linkedin.com/in/<slug>`
- Hygiene: `python cli.py dedupe-people`

## Cross-References (Code)
- Entry: `cli.py`, `pipelines/runner.py`
- Steps: `pipelines/steps/extract_data.py`, `validate_data.py`, `validate_people.py`, `persist_people.py`, `validate_companies.py`, `persist_companies.py`, `enrich_companies.py`
- Services: `services/llm_client.py`, `services/enrichment_service.py`, `services/mapping.py`, `services/domain_utils.py`, `services/reporting.py`
- Sources: `sources/linkedin_people.py`, `sources/google_maps_companies.py`, `sources/linkedin_companies_google.py`, `sources/registry.py`, `google_searcher.py`
- DB: `db/schema.py`, `db/repos/people_repo.py`, `db/repos/companies_repo.py`, `db/repos/outreach_repo.py`
- Config: `config/settings.py`, `config/llm_routes.py`

## How to Update
- When code changes significant flows or schemas, update the appropriate diagram and the Commands section. Keep diagrams short and focused per flow.

