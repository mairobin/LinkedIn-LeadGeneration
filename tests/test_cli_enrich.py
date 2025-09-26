from __future__ import annotations

import os
import sys
import sqlite3
from typing import Any, Dict, List


def _run_cli_with_args(args_list: List[str]) -> None:
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["cli.py"] + args_list
        if "cli" in sys.modules:
            del sys.modules["cli"]
        import cli  # type: ignore
        try:
            cli.main()  # type: ignore[attr-defined]
        except SystemExit as e:
            code = int(getattr(e, "code", 0) or 0)
            if code not in (0, None):
                raise
    finally:
        sys.argv = argv_backup


def test_cli_run_enrich_companies_with_stub(tmp_path, monkeypatch):
    # Test env: allow stub provider in RUN_ENV=test
    monkeypatch.setenv("RUN_ENV", "test")
    monkeypatch.setenv("AI_PROVIDER", "stub")
    # Prepare DB with one pending company
    db_path = tmp_path / "cli_enrich.db"
    _run_cli_with_args(["--db", str(db_path), "bootstrap"])

    # Seed a person such that a company row exists without enrichment
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("INSERT INTO companies(name, domain) VALUES(?, ?)", ("Acme GmbH", "acme.com"))
        conn.commit()
    finally:
        conn.close()

    # Run enrichment (stub fetcher allowed in RUN_ENV=test)
    _run_cli_with_args(["--db", str(db_path), "run", "enrich-companies", "--limit", "5"]) 

    # Verify enrichment wrote fields
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT legal_form, last_enriched_at FROM companies WHERE domain = ?", ("acme.com",))
        row = cur.fetchone()
        assert row is not None
        legal_form, last_enriched_at = row
        # Stub fetcher sets legal_form via derive; last_enriched_at should be set
        assert last_enriched_at is not None
    finally:
        conn.close()


