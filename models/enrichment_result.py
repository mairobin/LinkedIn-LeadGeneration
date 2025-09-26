from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class EnrichmentResult(BaseModel):
    """LLM structured output: strict shape expected from Linkup enrichment."""

    company: str = Field(alias="Company")
    legal_form: str | None = Field(default=None, alias="Legal_Form")
    industries: list[str] = Field(default_factory=list, alias="Industries")
    locations_germany: list[str] = Field(default_factory=list, alias="Locations_Germany")
    multinational: bool | None = Field(default=None, alias="Multinational")
    website: str | None = Field(default=None, alias="Website")
    size_employees: int | None = Field(default=None, alias="Size_Employees")
    business_model_key_points: list[str] = Field(default_factory=list, alias="Business_Model_Key_Points")
    products_and_services: list[str] = Field(default_factory=list, alias="Products_and_Services")
    recent_news: list[str] = Field(default_factory=list, alias="Recent_News")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


