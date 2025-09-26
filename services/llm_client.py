from __future__ import annotations

from typing import Any, Dict, List, Optional

from config.settings import get_settings
from config.llm_routes import ROUTES
from utils.llm_logger import log_call, sha256_text


class LLMClient:
    """Minimal wrapper to centralize per-use-case routing and logging."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def chat(self, *, use_case: str, messages: List[Dict[str, str]], temperature: Optional[float] = None, prompt_name: Optional[str] = None, prompt_text: Optional[str] = None) -> Any:
        route = ROUTES.get(use_case, {})
        provider = route.get("provider", "openai")
        model = route.get("model") or self.settings.openai_model or "gpt-4o-mini"
        op = route.get("operation", "chat")
        temp = temperature if temperature is not None else route.get("temperature")

        if provider != "openai":
            raise NotImplementedError(f"Provider not implemented: {provider}")

        from openai import OpenAI
        client = OpenAI(api_key=self.settings.openai_api_key)

        import time as _time
        _t0 = _time.time()
        kwargs: Dict[str, Any] = {"model": model, "messages": messages}
        # Only pass temperature if explicitly provided (some models only accept default)
        if temp is not None:
            kwargs["temperature"] = temp
        resp = client.chat.completions.create(**kwargs)
        _dt_ms = int((_time.time() - _t0) * 1000)

        usage_obj = None
        try:
            usage = getattr(resp, "usage", None)
            if usage:
                usage_obj = {
                    "prompt_tokens": getattr(usage, "prompt_tokens", None),
                    "completion_tokens": getattr(usage, "completion_tokens", None),
                    "total_tokens": getattr(usage, "total_tokens", None),
                }
        except Exception:
            usage_obj = None

        try:
            log_call(
                caller=f"llm_client.chat:{use_case}",
                provider=provider,
                model=model,
                operation=op,
                prompt_name=prompt_name,
                prompt_hash=sha256_text(prompt_text),
                duration_ms=_dt_ms,
                status="ok",
                usage=usage_obj,
            )
        except Exception:
            pass

        return resp

    # responses() removed: Responses API fallback no longer used

    def enrich_company(
        self,
        *,
        company_name: str,
        domain: Optional[str],
        user_message: str,
        prompt_name: Optional[str] = None,
        prompt_text: Optional[str] = None,
        provider_override: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Gateway method for company enrichment across providers.

        Routes by config.llm_routes (use_case: "company_enrichment") unless overridden.
        Returns a normalized dict matching our schema keys, or None on failure.
        """
        import time as _time
        import json as _json
        import re as _re

        def _extract_json(text: Optional[str]) -> Optional[Dict[str, Any]]:
            if not text:
                return None
            # Try raw parse first
            try:
                return _json.loads(text)
            except Exception:
                pass
            # Try fenced code block
            try:
                m = _re.search(r"```(?:json)?\n([\s\S]*?)\n```", text)
                if m:
                    return _json.loads(m.group(1))
            except Exception:
                pass
            # Try curly braces slice
            try:
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1 and end > start:
                    return _json.loads(text[start : end + 1])
            except Exception:
                pass
            return None

        route = ROUTES.get("company_enrichment", {})
        provider = (provider_override or route.get("provider") or self.settings.ai_provider or "openai").lower()
        model = route.get("model") or self.settings.openai_model or "gpt-4o-mini"
        op = route.get("operation", "company_enrichment")

        # Common logging envelope
        def _log(status: str, *, duration_ms: Optional[int] = None, error: Optional[str] = None, usage: Optional[Dict[str, Any]] = None) -> None:
            try:
                log_call(
                    caller=f"llm_client.enrich_company",
                    provider=provider,
                    model=model if provider == "openai" else None,
                    operation=op,
                    prompt_name=prompt_name,
                    prompt_hash=sha256_text(prompt_text),
                    duration_ms=duration_ms,
                    status=status,
                    error=error,
                    usage=usage,
                    extras={"company_name": company_name, "domain": domain},
                )
            except Exception:
                pass

        # Simple retry/backoff on transient failures
        max_attempts = 2
        backoff_ms = 300

        # Remove OpenAI enrichment path; Linkup only

        if provider == "linkup":
            try:
                from pydantic import BaseModel  # type: ignore
                from linkup import LinkupClient  # type: ignore
                # Use unified app schema for enrichment
                from models import EnrichmentResult as CompanyResearch  # type: ignore
            except Exception as e:
                _log("error", error=f"linkup import failed: {e}")
                return None

            api_key = self.settings.linkup_api_key
            if not api_key:
                _log("error", error="LINKUP_API_KEY missing")
                return None

            client = LinkupClient(api_key=api_key)
            last_err: Optional[str] = None
            for attempt in range(1, max_attempts + 1):
                _t0 = _time.time()
                try:
                    resp = client.search(
                        query=user_message,
                        depth="standard",
                        output_type="structured",
                        structured_output_schema=CompanyResearch,
                    )
                    dt = int((_time.time() - _t0) * 1000)
                    result: Optional[Dict[str, Any]] = None
                    if hasattr(resp, "model_dump"):
                        result = resp.model_dump()
                    elif isinstance(resp, dict):
                        result = resp
                    else:
                        try:
                            parsed = CompanyResearch.model_validate(resp)
                            result = parsed.model_dump()
                        except Exception:
                            result = None
                    _log("ok", duration_ms=dt)
                    return result
                except Exception as e:
                    dt = int((_time.time() - _t0) * 1000)
                    last_err = str(e)
                    _log("error", duration_ms=dt, error=last_err)
                    if attempt < max_attempts:
                        try:
                            _time.sleep(backoff_ms / 1000.0)
                        except Exception:
                            pass
                        continue
            return None

        raise NotImplementedError(f"Provider not implemented: {provider}")


