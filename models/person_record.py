from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PersonRecord(BaseModel):
    """App/DB record shape: used for persistence and internal flows."""

    linkedin_url: str
    full_name: str | None = None
    title: str | None = None
    company_name: str | None = None
    company_domain: str | None = None
    location: str | None = None
    connections_linkedin: int | None = None
    followers_linkedin: int | None = None
    email: str | None = None
    lookup_date: str | None = None

    model_config = ConfigDict(extra="ignore")


