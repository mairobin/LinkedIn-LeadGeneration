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
        connections_linkedin: Optional[int] = None,
        followers_linkedin: Optional[int] = None,
        website_info: Optional[str] = None,
        phone_info: Optional[str] = None,
        info_raw: Optional[str] = None,
        insights_text: Optional[str] = None,
        lookup_date: Optional[str] = None,
        source_name: Optional[str] = None,
        source_query: Optional[str] = None,
    ) -> int:
        """Insert or update a person by linkedin_profile; returns person id."""
        sql = (
            "INSERT INTO people (linkedin_profile, first_name, last_name, title_current, email, location_text, connections_linkedin, followers_linkedin, website_info, phone_info, info_raw, insights_text, lookup_date, source_name, source_query) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')), ?, ?) "
            "ON CONFLICT(linkedin_profile) DO UPDATE SET "
            " first_name = COALESCE(excluded.first_name, people.first_name), "
            " last_name = COALESCE(excluded.last_name, people.last_name), "
            " title_current = COALESCE(excluded.title_current, people.title_current), "
            " email = COALESCE(excluded.email, people.email), "
            " location_text = COALESCE(excluded.location_text, people.location_text), "
            " connections_linkedin = COALESCE(excluded.connections_linkedin, people.connections_linkedin), "
            " followers_linkedin = COALESCE(excluded.followers_linkedin, people.followers_linkedin), "
            " website_info = COALESCE(excluded.website_info, people.website_info), "
            " phone_info = COALESCE(excluded.phone_info, people.phone_info), "
            " info_raw = COALESCE(excluded.info_raw, people.info_raw), "
            " insights_text = COALESCE(excluded.insights_text, people.insights_text), "
            " lookup_date = COALESCE(excluded.lookup_date, people.lookup_date), "
            " source_name = COALESCE(excluded.source_name, people.source_name), "
            " source_query = COALESCE(excluded.source_query, people.source_query) "
            "RETURNING id;"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (
            linkedin_profile, first_name, last_name, title_current, email, location_text, connections_linkedin, followers_linkedin, website_info, phone_info, info_raw, insights_text, lookup_date, source_name, source_query
        ))
        row = cur.fetchone()
        return int(row[0])

    def link_person_to_company(self, linkedin_profile: str, company_id: int) -> None:
        """Associate a person row to a company by ids."""
        sql = "UPDATE people SET company_id = ? WHERE linkedin_profile = ?;"
        self.conn.execute(sql, (company_id, linkedin_profile))
        self.conn.commit()

    # --- Normalized names (wrappers) ---
    def upsert(self, **kwargs) -> int:
        """Normalized wrapper alias for upserting a person."""
        return self.upsert_person(**kwargs)




