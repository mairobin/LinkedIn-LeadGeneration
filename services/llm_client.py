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
        temp = temperature if temperature is not None else route.get("temperature", 0)

        if provider != "openai":
            raise NotImplementedError(f"Provider not implemented: {provider}")

        from openai import OpenAI
        client = OpenAI(api_key=self.settings.openai_api_key)

        import time as _time
        _t0 = _time.time()
        resp = client.chat.completions.create(
            model=model,
            temperature=temp,
            messages=messages,
        )
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

    def responses(self, *, use_case: str, input_text: str, tools: Optional[List[Dict[str, Any]]] = None, prompt_name: Optional[str] = None) -> Any:
        route = ROUTES.get(use_case, {})
        provider = route.get("provider", "openai")
        model = route.get("model") or self.settings.openai_model or "gpt-4o-mini"
        op = route.get("operation", "responses")

        if provider != "openai":
            raise NotImplementedError(f"Provider not implemented: {provider}")

        from openai import OpenAI
        client = OpenAI(api_key=self.settings.openai_api_key)

        import time as _time
        _t0 = _time.time()
        resp = client.responses.create(
            model=model,
            input=input_text,
            tools=tools or [],
        )
        _dt_ms = int((_time.time() - _t0) * 1000)

        try:
            log_call(
                caller=f"llm_client.responses:{use_case}",
                provider=provider,
                model=model,
                operation=op,
                prompt_name=prompt_name,
                duration_ms=_dt_ms,
                status="ok",
            )
        except Exception:
            pass

        return resp


