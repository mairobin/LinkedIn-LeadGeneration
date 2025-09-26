from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:  # type: ignore
        return None


def _load_env() -> None:
    # Centralized dotenv loading; safe if .env missing
    load_dotenv()


@dataclass(frozen=True)
class Settings:
    google_api_key: str | None
    google_cse_id: str | None
    google_search_url: str

    search_delay_seconds: int
    max_retries: int
    request_timeout_seconds: int

    default_max_results: int
    results_per_page: int

    raw_output_dir: str
    processed_output_dir: str

    log_level: str

    # Core/runtime
    db_path: str
    run_env: str

    openai_api_key: str | None
    openai_model: str | None
    linkup_api_key: str | None

    # AI gating
    ai_enabled: bool
    ai_provider: str  # stub | openai | linkup

    # Content/extraction
    linkedin_url_patterns: list[str]
    extraction_version: str
    required_fields: list[str]
    optional_fields: list[str]

    # Limits/Concurrency/Timeouts
    search_rate_limit_qps: float
    enrich_concurrency: int
    http_timeout_seconds: int

    # Optional: Google Places / Maps (for future company sources)
    google_places_api_key: str | None = None
    google_places_text_search_url: str = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    google_places_details_url: str = "https://maps.googleapis.com/maps/api/place/details/json"

    # Logging/tracing
    llm_trace: bool = False
    llm_log_path: str = "logs/llm_calls.jsonl"

    # Feature flags
    demo: bool = False


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _load_env()
    ai_enabled = os.getenv("AI_ENABLED", "false").lower() == "true"
    ai_provider = os.getenv("AI_PROVIDER", "stub")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    linkup_api_key = os.getenv("LINKUP_API_KEY")

    if ai_enabled:
        if ai_provider == "openai" and not openai_api_key:
            raise RuntimeError(
                "OPENAI_API_KEY required when AI_PROVIDER=openai and AI_ENABLED=true"
            )
        if ai_provider == "linkup" and not linkup_api_key:
            raise RuntimeError(
                "LINKUP_API_KEY required when AI_PROVIDER=linkup and AI_ENABLED=true"
            )
    return Settings(
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        google_cse_id=os.getenv("GOOGLE_CSE_ID"),
        google_search_url=os.getenv("GOOGLE_SEARCH_URL", "https://www.googleapis.com/customsearch/v1"),
        search_delay_seconds=int(os.getenv("SEARCH_DELAY", "1")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT", "30")),
        default_max_results=int(os.getenv("DEFAULT_MAX_RESULTS", "10")),
        results_per_page=int(os.getenv("RESULTS_PER_PAGE", "10")),
        raw_output_dir=os.getenv("RAW_OUTPUT_DIR", "output"),
        processed_output_dir=os.getenv("PROCESSED_OUTPUT_DIR", "processed"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        db_path=os.getenv("DB_PATH", "leads.db"),
        run_env=os.getenv("RUN_ENV", "local"),
        openai_api_key=openai_api_key,
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        linkup_api_key=linkup_api_key,
        ai_enabled=ai_enabled,
        ai_provider=ai_provider,
        linkedin_url_patterns=[
            "linkedin.com/in/",
            "de.linkedin.com/in/",
            "www.linkedin.com/in/",
        ],
        extraction_version=os.getenv("EXTRACTION_VERSION", "1.0"),
        required_fields=["name", "profile_url", "summary"],
        optional_fields=[
            "current_position",
            "company",
            "location",
            "follower_count",
            "connection_count",
            "email",
            "website",
            "phone",
            "experience_years",
            "summary_other",
        ],
        search_rate_limit_qps=float(os.getenv("SEARCH_RATE_LIMIT_QPS", "2")),
        enrich_concurrency=int(os.getenv("ENRICH_CONCURRENCY", "2")),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
        google_places_api_key=os.getenv("GOOGLE_PLACES_API_KEY"),
        google_places_text_search_url=os.getenv("GOOGLE_PLACES_TEXT_SEARCH_URL", "https://maps.googleapis.com/maps/api/place/textsearch/json"),
        google_places_details_url=os.getenv("GOOGLE_PLACES_DETAILS_URL", "https://maps.googleapis.com/maps/api/place/details/json"),
        llm_trace=os.getenv("LLM_TRACE", "false").lower() in ("1", "true", "yes", "on"),
        llm_log_path=os.getenv("LLM_LOG_PATH", "logs/llm_calls.jsonl"),
        demo=os.getenv("DEMO", "false").lower() in ("1", "true", "yes", "on"),
    )


