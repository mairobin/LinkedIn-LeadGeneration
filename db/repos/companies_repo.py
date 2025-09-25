from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


class CompaniesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_by_domain(self, name: Optional[str], domain: Optional[str], website: Optional[str]) -> int:
        # Manual upsert to avoid relying on UNIQUE constraint semantics
        cur = self.conn.cursor()
        # Ensure we never insert a NULL name to satisfy stricter schemas
        safe_name = name or domain or "Unknown Company"
        if domain:
            cur.execute("SELECT id, name, website FROM companies WHERE domain = ?", (domain,))
            row = cur.fetchone()
            if row:
                company_id = int(row[0])
                # Update minimal fields when provided
                cur.execute(
                    "UPDATE companies SET name = COALESCE(?, name), website = COALESCE(?, website) WHERE id = ?",
                    (safe_name, website, company_id),
                )
                self.conn.commit()
                return company_id
            # Insert new with domain
            cur.execute(
                "INSERT INTO companies (name, domain, website) VALUES (?, ?, ?)",
                (safe_name, domain, website),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        # No domain yet: insert a stub with name only (duplicates allowed)
        cur.execute("INSERT INTO companies (name) VALUES (?)", (safe_name,))
        self.conn.commit()
        return int(cur.lastrowid)

    def update_enrichment(self, company_id: int, fields: Dict[str, Any]) -> None:
        columns = []
        values: List[Any] = []
        for key in [
            "legal_form",
            "industries_json",
            "locations_de_json",
            "multinational",
            "domain",
            "website",
            "size_employees",
            "business_model_json",
            "products_json",
            "recent_news_json",
        ]:
            if key in fields:
                if key.endswith("_json") and isinstance(fields[key], (list, dict)):
                    columns.append(f"{key} = json(?)")
                    import json as _json
                    # Preserve non-ASCII characters (e.g., umlauts) in stored JSON text
                    values.append(_json.dumps(fields[key], ensure_ascii=False))
                else:
                    columns.append(f"{key} = ?")
                    values.append(fields[key])
        columns.append("last_enriched_at = datetime('now')")
        sql = f"UPDATE companies SET {', '.join(columns)} WHERE id = ?;"
        values.append(company_id)
        self.conn.execute(sql, tuple(values))
        self.conn.commit()

    def select_pending_enrichment(self, limit: int = 50) -> List[Tuple]:
        sql = (
            "SELECT id, name, domain FROM companies "
            "WHERE last_enriched_at IS NULL "
            "   OR (industries_json IS NULL OR json_array_length(COALESCE(industries_json, '[]')) = 0) "
            "   OR size_employees IS NULL "
            "ORDER BY name LIMIT ?;"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (limit,))
        return cur.fetchall()


