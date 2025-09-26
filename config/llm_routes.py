from __future__ import annotations

import os


# Central routing for LLM use-cases. Edit here to change per-operation defaults.
# You can also override per-route model via env vars for quick testing.
#
# Keys are use_case identifiers consumed by services/llm_client.py
ROUTES: dict[str, dict] = {
    # Company enrichment (provider-routed; default Linkup)
    "company_enrichment": {
        "provider": os.getenv("LLM_ENRICHMENT_PROVIDER", "linkup"),
        "model": os.getenv("OPENAI_MODEL_ENRICHMENT"),  # falls back to global OPENAI_MODEL (when provider is openai)
        "temperature": 0,
        # Logical operation name for logging (not a vendor API name)
        "operation": "company_enrichment",
    },
    # Profile extraction (OpenAI chat)
    "profile_extraction": {
        "provider": os.getenv("LLM_PROFILE_PROVIDER", "openai"),
        "model": os.getenv("OPENAI_MODEL_PROFILE"),
        "operation": "profile_extraction",
    },
}


