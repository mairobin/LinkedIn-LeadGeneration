from __future__ import annotations

from typing import Dict, Iterable
import sqlite3

from db.repos.companies_repo import CompaniesRepo


def ingest_companies(conn: sqlite3.Connection, companies: Iterable[Dict]) -> int:
    repo = CompaniesRepo(conn)
    processed = 0
    for c in companies:
        name = c.get("Company") or c.get("name")
        website = c.get("Company_Website") or c.get("website")
        domain = c.get("Company_Domain") or c.get("domain")
        source_name = c.get("source_name") or None
        source_query = c.get("source_query") or None
        if not (name or domain or website):
            continue
        try:
            repo.upsert_by_domain(name, domain, website, source_name=source_name, source_query=source_query)
            processed += 1
        except Exception:
            # Best-effort ingestion; skip faulty entries
            pass
    return processed


