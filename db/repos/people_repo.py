from __future__ import annotations

import sqlite3
from typing import Any, Dict, Optional


class PeopleRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_person(
        self,
        linkedin_profile: str,
        first_name: Optional[str],
        last_name: Optional[str],
        title_current: Optional[str],
        email: Optional[str],
        location_text: Optional[str],
    ) -> int:
        sql = (
            "INSERT INTO people (linkedin_profile, first_name, last_name, title_current, email, location_text) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(linkedin_profile) DO UPDATE SET "
            " first_name = COALESCE(excluded.first_name, people.first_name), "
            " last_name = COALESCE(excluded.last_name, people.last_name), "
            " title_current = COALESCE(excluded.title_current, people.title_current), "
            " email = COALESCE(excluded.email, people.email), "
            " location_text = COALESCE(excluded.location_text, people.location_text) "
            "RETURNING id;"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (linkedin_profile, first_name, last_name, title_current, email, location_text))
        row = cur.fetchone()
        return int(row[0])

    def link_person_to_company(self, linkedin_profile: str, company_id: int) -> None:
        sql = "UPDATE people SET company_id = ? WHERE linkedin_profile = ?;"
        self.conn.execute(sql, (company_id, linkedin_profile))
        self.conn.commit()



