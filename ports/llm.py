from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class LLMClientPort(Protocol):
    def chat(
        self,
        *,
        use_case: str,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        prompt_name: Optional[str] = None,
        prompt_text: Optional[str] = None,
    ) -> Any:
        ...

    def responses(
        self,
        *,
        use_case: str,
        input_text: str,
        tools: Optional[List[Dict[str, Any]]] = None,
        prompt_name: Optional[str] = None,
    ) -> Any:
        ...


