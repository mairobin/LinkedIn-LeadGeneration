from __future__ import annotations

import sqlite3

from db import schema
from db.repos.companies_repo import CompaniesRepo


def test_update_enrichment_skips_conflicting_domain(tmp_path):
    db = sqlite3.connect(str(tmp_path / "t.db"))
    try:
        schema.bootstrap(db)
        repo = CompaniesRepo(db)
        a = repo.upsert_company("Acme", "acme.com", "https://acme.com")
        b = repo.upsert_company("Beta", "beta.io", "https://beta.io")
        assert a != b
        # Attempt to change Beta's domain to Acme's domain (should be skipped)
        repo.save_company_enrichment(b, {"domain": "acme.com", "legal_form": "GmbH"})

        cur = db.cursor()
        cur.execute("SELECT domain, legal_form FROM companies WHERE id = ?", (b,))
        domain, legal = cur.fetchone()
        assert domain == "beta.io"  # unchanged due to conflict
        assert legal == "GmbH"
    finally:
        db.close()


