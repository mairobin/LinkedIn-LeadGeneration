from __future__ import annotations

from typing import Dict, List
import sqlite3

from db.repos.companies_repo import CompaniesRepo
from pipelines.runner import RunContext


class PersistCompanies:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def run(self, ctx: RunContext) -> RunContext:
        companies: List[Dict] = ctx.companies or []
        repo = CompaniesRepo(self.conn)
        processed = 0
        for c in companies:
            try:
                name = c.get("Company") or c.get("name")
                website = c.get("Company_Website") or c.get("website")
                domain = c.get("Company_Domain") or c.get("domain")
                source_name = c.get("source_name") or None
                source_query = c.get("source_query") or None
                if not (name or domain or website):
                    continue
                repo.upsert_by_domain(name, domain, website, source_name=source_name, source_query=source_query)
                processed += 1
            except Exception:
                # Best-effort persistence; skip faulty entries
                pass
        ctx.meta["processed_companies"] = processed
        return ctx


