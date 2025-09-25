"""
SQLite storage utilities for LinkedIn Lead Generation.

Stores Person records using the new schema and upserts rows using the
stable unique key (LinkedIn_Profile).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
import sqlite3


# Canonical column order for the Person schema.
COLUMNS: List[str] = [
    "LinkedIn_Profile",
    "Contact_Name",
    "Company",
    "Company_Website",
    "Company_Domain",
    "Location",
    "Position",
    "Connections_LinkedIn",
    "Followers_LinkedIn",
    "Website_Info",
    "Phone_Info",
    "Info_raw",
    "Insights",  # list → semicolon-joined string for storage
    "Email",
    "Lookup_Date",
    "Hot",  # boolean → integer 0/1
    "Last_Interaction_Date",
    "Status",
    "Notes",  # list → semicolon-joined string for storage
]


def _normalize_list_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join([str(x) for x in value if x is not None and str(x).strip()])
    return str(value)


def _profile_to_values(profile: Dict[str, Any]) -> Tuple[Any, ...]:
    values: List[Any] = []
    for key in COLUMNS:
        v = profile.get(key, "")
        if key in ("Insights", "Notes"):
            v = _normalize_list_field(v)
        elif key == "Hot":
            v = 1 if bool(v) else 0
        if v is None:
            v = ""
        values.append(v)
    return tuple(values)


class SQLiteStorage:
    def __init__(self, db_path: str, table_name: str = "profiles"):
        self.db_path = db_path
        self.table_name = table_name
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def _ensure_schema(self) -> None:
        columns_sql = ",\n".join([
            "    LinkedIn_Profile TEXT NOT NULL",
            "    Contact_Name TEXT",
            "    Company TEXT",
            "    Company_Website TEXT",
            "    Company_Domain TEXT",
            "    Location TEXT",
            "    Position TEXT",
            "    Connections_LinkedIn INTEGER",
            "    Followers_LinkedIn INTEGER",
            "    Website_Info TEXT",
            "    Phone_Info TEXT",
            "    Info_raw TEXT",
            "    Insights TEXT",
            "    Email TEXT",
            "    Lookup_Date TEXT",
            "    Hot INTEGER",
            "    Last_Interaction_Date TEXT",
            "    Status TEXT",
            "    Notes TEXT",
        ])
        create_sql = f"
CREATE TABLE IF NOT EXISTS {self.table_name} (
{columns_sql},
    CONSTRAINT uq_profile UNIQUE (LinkedIn_Profile)
);
".strip()
        self._conn.execute(create_sql)
        # Add Company_Domain column if missing (safe, idempotent)
        try:
            self._conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN Company_Domain TEXT;")
        except Exception:
            pass
        self._conn.commit()

    def upsert_profiles(self, profiles: Iterable[Dict[str, Any]]) -> int:
        """Upsert multiple profiles. Returns number of rows affected (approx)."""
        placeholders = ", ".join(["?" for _ in COLUMNS])
        set_clause = ", ".join([f"{col}=excluded.{col}" for col in COLUMNS if col != "LinkedIn_Profile"])
        sql = f"""
INSERT INTO {self.table_name} ({', '.join(COLUMNS)})
VALUES ({placeholders})
ON CONFLICT(LinkedIn_Profile) DO UPDATE SET {set_clause};
"""
        values = [_profile_to_values(p) for p in profiles]
        if not values:
            return 0
        cur = self._conn.cursor()
        cur.executemany(sql, values)
        self._conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0

    def fetch_sample(self, limit: int = 5) -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute(f"SELECT {', '.join(COLUMNS)} FROM {self.table_name} ORDER BY rowid DESC LIMIT ?", (limit,))
        rows = cur.fetchall()
        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append({key: row[idx] for idx, key in enumerate(COLUMNS)})
        return results



