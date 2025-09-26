from __future__ import annotations

import json
import os
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _as_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def sha256_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    try:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()
    except Exception:
        return None


def log_call(
    *,
    caller: str,
    provider: str,
    model: Optional[str],
    operation: str,
    prompt_name: Optional[str] = None,
    prompt_hash: Optional[str] = None,
    duration_ms: Optional[int] = None,
    status: str = "ok",
    error: Optional[str] = None,
    usage: Optional[Dict[str, Any]] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    """Append a single JSON line describing an LLM call if tracing is enabled.

    Controlled by env variables:
      - LLM_TRACE: enable/disable logging (default: false)
      - LLM_LOG_PATH: output file path (default: logs/llm_calls.jsonl)
    """
    if not _as_bool(os.getenv("LLM_TRACE", "false")):
        return

    log_path = Path(os.getenv("LLM_LOG_PATH", "logs/llm_calls.jsonl"))
    _ensure_parent_dir(log_path)

    payload: Dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "caller": caller,
        "provider": provider,
        "model": model,
        "operation": operation,
        "prompt_name": prompt_name,
        "prompt_hash": prompt_hash,
        "duration_ms": duration_ms,
        "status": status,
        "error": error,
        "usage": usage or {},
    }
    # Include run metadata if present
    run_id = os.getenv("RUN_ID")
    if run_id:
        payload["run_id"] = run_id

    if extras:
        # Shallow merge extras under a dedicated key to avoid collisions
        payload["extras"] = extras

    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Never break the app on logging failures
        return

