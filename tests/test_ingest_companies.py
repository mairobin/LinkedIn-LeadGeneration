from __future__ import annotations

import sqlite3

from db import schema
from pipelines.runner import Pipeline, RunContext
from pipelines.steps.validate_companies import ValidateCompanies
from pipelines.steps.persist_companies import PersistCompanies


def test_ingest_companies_idempotent_by_domain(tmp_path):
    db_path = tmp_path / "t.db"
    conn = sqlite3.connect(str(db_path))
    try:
        schema.bootstrap(conn)
        companies = [
            {"Company": "Acme", "Company_Domain": "acme.com", "Company_Website": "https://acme.com"},
            {"Company": "ACME GmbH", "Company_Domain": "acme.com"},
        ]
        # Run pipeline twice to ensure idempotency of DB state
        ctx1 = RunContext()
        ctx1.companies = list(companies)
        Pipeline([ValidateCompanies(), PersistCompanies(conn)]).run(ctx1)
        ctx2 = RunContext()
        ctx2.companies = list(companies)
        Pipeline([ValidateCompanies(), PersistCompanies(conn)]).run(ctx2)
        # Rows processed counts include attempts; idempotency refers to DB state
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM companies WHERE domain = ?", ("acme.com",))
        n = cur.fetchone()[0]
        assert n == 1
    finally:
        conn.close()



