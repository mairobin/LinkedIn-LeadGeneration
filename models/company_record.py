from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class CompanyRecord(BaseModel):
    """App/DB record shape: used for persistence and internal flows."""

    name: str
    domain: str | None = None
    website: str | None = None
    size_employees: int | None = None
    legal_form: str | None = None

    model_config = ConfigDict(extra="ignore")


