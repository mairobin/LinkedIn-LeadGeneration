from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional


def _llm_usage_for_run(run_id: str) -> Dict[str, Dict[str, int]]:
    """Aggregate LLM usage from logs/llm_calls.jsonl for the given run_id.

    Returns dict like { 'openai': {'calls': N, 'tokens': T}, 'linkup': {...} }
    """
    result: Dict[str, Dict[str, int]] = {}
    try:
        from config.settings import get_settings
        settings = get_settings()
        log_path = Path(settings.llm_log_path)
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


def print_summary(data: dict, api_usage: dict, output_path: Optional[Path] = None) -> None:
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
        import os
        from config.settings import get_settings
        settings = get_settings()
        run_id = os.getenv("RUN_ID")
        if run_id and settings.llm_trace:
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


