from __future__ import annotations

import sqlite3
from typing import Optional


def _normalize_query_text(query_text: Optional[str]) -> str:
    if not query_text:
        return ""
    # Lowercase, trim, and collapse internal whitespace to a single space
    lowered = str(query_text).lower().strip()
    # Collapse consecutive whitespace
    parts = lowered.split()
    return " ".join(parts)


class QueriesRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def find_or_create(self, source: str, entity_type: str, query_text: str) -> int:
        """Return id of canonical query row for (source, entity_type, normalized_query)."""
        normalized = _normalize_query_text(query_text)
        cur = self.conn.cursor()
        cur.execute(
            (
                "INSERT OR IGNORE INTO search_queries (source, entity_type, query_text, normalized_query) "
                "VALUES (?, ?, ?, ?)"
            ),
            (source or "", entity_type, query_text or "", normalized),
        )
        # Now select id (works whether inserted or already existed)
        cur.execute(
            (
                "SELECT id FROM search_queries WHERE source = ? AND entity_type = ? AND normalized_query = ?"
            ),
            (source or "", entity_type, normalized),
        )
        row = cur.fetchone()
        if not row:
            # Should not happen due to INSERT OR IGNORE + same select, but guard anyway
            raise RuntimeError("Failed to find_or_create search_query row")
        query_id = int(row[0])
        try:
            # Best-effort update of last_executed_at
            self.conn.execute(
                "UPDATE search_queries SET last_executed_at = datetime('now') WHERE id = ?",
                (query_id,),
            )
        except Exception:
            pass
        self.conn.commit()
        return query_id


