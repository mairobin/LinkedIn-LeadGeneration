from __future__ import annotations

import os
import sys
import sqlite3
from typing import Any, Dict, List

import importlib


def _run_main_with_args(args_list: List[str]) -> None:
    """Simulate CLI execution of main.py with given args (non-interactive)."""
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["main.py"] + args_list
        # Reload main fresh to re-parse args and pick up monkeypatches
        if "main" in sys.modules:
            del sys.modules["main"]
        import main  # noqa: F401
        try:
            # Explicitly run the CLI entrypoint
            main.main()  # type: ignore[attr-defined]
        except SystemExit as e:
            # Treat sys.exit(0) as success; re-raise non-zero for visibility
            if int(getattr(e, "code", 0) or 0) not in (0, None):
                raise
    finally:
        sys.argv = argv_backup


def test_e2e_person_source_writes_people_and_companies(tmp_path, monkeypatch):
    # Ensure no real API/AI usage
    monkeypatch.setenv("AI_ENABLED", "false")
    monkeypatch.setenv("RUN_ENV", "test")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_CSE_ID", raising=False)

    # Stub the registry to return a fake people source that avoids network/AI
    import sources.registry as reg

    class _StubPeopleSource:
        source_name = "linkedin_people_google"
        entity_type = "person"

        def run(self, terms: List[str], max_results: int) -> List[Dict[str, Any]]:
            # Minimal realistic person payload expected by downstream mapping/ingestion
            return [
                {
                    "name": "Alice Example",
                    "profile_url": "https://www.linkedin.com/in/Alice-Example-12345/",
                    "current_position": "Software Engineer",
                    "company": "Acme GmbH",
                    "location": "Berlin, Germany",
                    "follower_count": "1K",
                    "connection_count": "500+",
                    # ensure domain is preserved via map_to_person_schema
                    "company_domain": "acme.com",
                }
            ]

    # Swap the registry mapping to only include our stubbed people source
    monkeypatch.setattr(reg, "_REGISTRY", {"linkedin_people_google": lambda: _StubPeopleSource()})

    # Prepare temp DB path and run main with write-db enabled
    db_path = tmp_path / "e2e.db"
    _run_main_with_args([
        "--query", "Engineer Berlin",
        "--max-results", "1",
        "--source", "linkedin_people_google",
        "--write-db",
        "--db-path", str(db_path),
        "--log-level", "ERROR",
    ])

    # Verify DB content: one person, one company, linked via company_id, provenance set
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        # People
        cur.execute("SELECT linkedin_profile, first_name, last_name, title_current, company_id, source_name, source_query FROM people;")
        people_rows = cur.fetchall()
        assert len(people_rows) == 1
        linkedin_profile, first_name, last_name, title_current, company_id, p_source_name, p_source_query = people_rows[0]
        # URL normalized to canonical /in/{slug}
        assert linkedin_profile.startswith("https://linkedin.com/in/")
        assert first_name == "Alice"
        assert last_name == "Example"
        assert title_current == "Software Engineer"
        assert company_id is not None
        assert p_source_name == "linkedin_people_google"
        assert isinstance(p_source_query, str) and "Engineer" in p_source_query

        # Companies
        cur.execute("SELECT id, name, domain, website, source_name, source_query FROM companies;")
        comp_rows = cur.fetchall()
        assert len(comp_rows) == 1
        c_id, c_name, c_domain, c_website, c_source_name, c_source_query = comp_rows[0]
        assert c_id == company_id
        assert c_name == "Acme GmbH"
        # domain provided via stubbed company_domain
        assert c_domain == "acme.com"
        # website may be NULL because map_to_person_schema does not carry website
        assert c_source_name == "linkedin_people_google"
        assert c_source_query == p_source_query

    finally:
        conn.close()
