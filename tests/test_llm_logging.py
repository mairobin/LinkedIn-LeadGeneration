from __future__ import annotations

import json
from pathlib import Path

from utils.llm_logger import log_call


def test_llm_trace_writes_jsonl(tmp_path, monkeypatch):
    log_file = tmp_path / "llm_calls.jsonl"
    monkeypatch.setenv("LLM_TRACE", "true")
    monkeypatch.setenv("LLM_LOG_PATH", str(log_file))
    monkeypatch.setenv("RUN_ID", "test-run-123")

    log_call(
        caller="unit.test",
        provider="openai",
        model="gpt-x",
        operation="chat.completions.create",
        prompt_name="demo",
        prompt_hash="abc",
        duration_ms=42,
        status="ok",
        usage={"total_tokens": 10},
        extras={"company_name": "Acme"},
    )

    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 1
    rec = json.loads(lines[-1])
    assert rec["caller"] == "unit.test"
    assert rec["provider"] == "openai"
    assert rec["operation"] == "chat.completions.create"
    assert rec["run_id"] == "test-run-123"
    assert rec.get("usage", {}).get("total_tokens") == 10


