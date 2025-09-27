from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


class CompaniesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_by_domain(self, name: Optional[str], domain: Optional[str], website: Optional[str], source_name: Optional[str] = None, source_query: Optional[str] = None, search_query_id: Optional[int] = None) -> int:
        """Insert or update a company row using its domain as the stable key.

        Returns the company id.
        """
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
                    "UPDATE companies SET name = COALESCE(?, name), website = COALESCE(?, website), source_name = COALESCE(?, source_name), source_query = COALESCE(?, source_query), search_query_id = COALESCE(?, search_query_id) WHERE id = ?",
                    (safe_name, website, source_name, source_query, search_query_id, company_id),
                )
                self.conn.commit()
                return company_id
            # Insert new with domain
            cur.execute(
                "INSERT INTO companies (name, domain, website, source_name, source_query, search_query_id) VALUES (?, ?, ?, ?, ?, ?)",
                (safe_name, domain, website, source_name, source_query, search_query_id),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        # No domain yet: insert a stub with name only (duplicates allowed)
        cur.execute("INSERT INTO companies (name, source_name, source_query, search_query_id) VALUES (?, ?, ?, ?)", (safe_name, source_name, source_query, search_query_id))
        self.conn.commit()
        return int(cur.lastrowid)

    def update_enrichment(self, company_id: int, fields: Dict[str, Any]) -> None:
        """Update enrichment-related fields for a company by id.

        Fields may include *_json arrays which will be stored as JSON text.
        """
        # Avoid UNIQUE(domain) conflicts by skipping domain updates that collide with another row
        safe_fields: Dict[str, Any] = dict(fields)
        try:
            if safe_fields.get("domain"):
                cur = self.conn.cursor()
                cur.execute("SELECT id FROM companies WHERE domain = ?", (safe_fields["domain"],))
                row = cur.fetchone()
                if row and int(row[0]) != int(company_id):
                    # Another company already owns this domain; skip updating domain for this row
                    safe_fields.pop("domain", None)
        except Exception:
            # Best-effort safeguard; proceed without altering fields on error
            pass

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
            if key in safe_fields:
                if key.endswith("_json") and isinstance(safe_fields[key], (list, dict)):
                    columns.append(f"{key} = json(?)")
                    import json as _json
                    # Preserve non-ASCII characters (e.g., umlauts) in stored JSON text
                    values.append(_json.dumps(safe_fields[key], ensure_ascii=False))
                else:
                    columns.append(f"{key} = ?")
                    values.append(safe_fields[key])
        columns.append("last_enriched_at = datetime('now')")
        sql = f"UPDATE companies SET {', '.join(columns)} WHERE id = ?;"
        values.append(company_id)
        self.conn.execute(sql, tuple(values))
        self.conn.commit()

    def select_pending_enrichment(self, limit: int = 50) -> List[Tuple]:
        """Return (id, name, domain) tuples for companies missing enrichment."""
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

    # --- Normalized names (wrappers) ---
    def upsert_company(self, name: Optional[str], domain: Optional[str], website: Optional[str], source_name: Optional[str] = None, source_query: Optional[str] = None, search_query_id: Optional[int] = None) -> int:
        """Normalized wrapper for inserting/updating a company."""
        return self.upsert_by_domain(name, domain, website, source_name, source_query, search_query_id)

    def save_company_enrichment(self, company_id: int, fields: Dict[str, Any]) -> None:
        """Normalized wrapper to persist enrichment fields for a company id."""
        self.update_enrichment(company_id, fields)

    def get_pending_companies(self, limit: int = 50) -> List[Tuple[int, Optional[str], Optional[str]]]:
        """Normalized wrapper: companies pending enrichment.

        Returns a list of (id, name, domain).
        """
        return self.select_pending_enrichment(limit)


