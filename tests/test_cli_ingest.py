from __future__ import annotations

import os
import sys
import sqlite3
from typing import Any, Dict, List


def _run_cli_with_args(args_list: List[str]) -> None:
    """Run cli.py main() with provided argv in-process (no subprocess)."""
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["cli.py"] + args_list
        # Import fresh to ensure clean parser each time
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


def test_cli_run_ingest_profiles_writes_db(tmp_path, monkeypatch):
    # Local test environment
    monkeypatch.setenv("RUN_ENV", "test")
    monkeypatch.setenv("AI_ENABLED", "false")
    # Stub registry to avoid network
    import sources.registry as reg

    class _StubPeopleSource:
        source_name = "linkedin_people_google"
        entity_type = "person"

        def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
            return [
                {
                    "name": "Alice Example",
                    "profile_url": "https://www.linkedin.com/in/Alice-Example-12345/",
                    "current_position": "Software Engineer",
                    "company": "Acme GmbH",
                    "company_domain": "acme.com",
                    "location": "Berlin, Germany",
                    "follower_count": "1K",
                    "connection_count": "500+",
                }
            ]

    monkeypatch.setattr(reg, "_REGISTRY", {"linkedin_people_google": lambda: _StubPeopleSource()})

    db_path = tmp_path / "cli_ingest.db"
    # Bootstrap schema
    _run_cli_with_args(["--db", str(db_path), "bootstrap"])
    # Ingest 1 profile
    _run_cli_with_args([
        "--db", str(db_path),
        "run", "ingest-people",
        "--query", "Engineer Berlin",
        "--max-results", "1",
        "--write-db",
    ])

    # Verify DB state
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM people")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT name, domain FROM companies")
        rows = cur.fetchall()
        assert rows == [("Acme GmbH", "acme.com")]
    finally:
        conn.close()


