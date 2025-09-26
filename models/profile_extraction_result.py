from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ProfileExtractionResult(BaseModel):
    """LLM structured output: strict shape expected from profile extraction."""

    current_position: str | None = None
    company: str | None = None
    location: str | None = None
    follower_count: str | None = None
    connection_count: str | None = None
    company_website: str | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


