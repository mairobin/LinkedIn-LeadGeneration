from __future__ import annotations

import sqlite3

from db import schema
from pipelines.ingest_companies import ingest_companies


def test_ingest_companies_idempotent_by_domain(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_path))
    try:
        schema.bootstrap(conn)
        companies = [
            {"Company": "Acme", "Company_Domain": "acme.com", "Company_Website": "https://acme.com"},
            {"Company": "ACME GmbH", "Company_Domain": "acme.com"},
        ]
        count1 = ingest_companies(conn, companies)
        count2 = ingest_companies(conn, companies)
        # Rows processed counts include attempts; idempotency refers to DB state
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM companies WHERE domain = ?", ("acme.com",))
        n = cur.fetchone()[0]
        assert n == 1
    finally:
        conn.close()



