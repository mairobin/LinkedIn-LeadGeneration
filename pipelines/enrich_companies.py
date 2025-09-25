from __future__ import annotations

import json
from typing import Dict, List
import sqlite3

from db.repos.companies_repo import CompaniesRepo


def enrich_batch(conn: sqlite3.Connection, fetch_func) -> int:
    """Fetch pending companies, call fetch_func(name, domain) -> enrichment dict, persist.

    fetch_func must return a dict matching the plan keys, e.g.:
    {
      "Company": str,
      "Legal_Form": str|None,
      "Industries": [str],
      "Locations_Germany": [str],
      "Multinational": bool,
      "Website": str|None,
      "Size_Employees": int|None,
      "Business_Model_Key_Points": [str],
      "Products_and_Services": [str],
      "Recent_News": [str]
    }
    """
    repo = CompaniesRepo(conn)
    rows = repo.select_pending_enrichment(limit=50)
    updated = 0
    for (company_id, name, domain) in rows:
        data = fetch_func(name, domain)
        if not isinstance(data, dict):
            continue
        fields = {
            "legal_form": data.get("Legal_Form"),
            "industries_json": data.get("Industries"),
            "locations_de_json": data.get("Locations_Germany"),
            "multinational": 1 if data.get("Multinational") else 0,
            "domain": domain or None,
            "website": data.get("Website"),
            "size_employees": data.get("Size_Employees"),
            "business_model_json": data.get("Business_Model_Key_Points"),
            "products_json": data.get("Products_and_Services"),
            "recent_news_json": data.get("Recent_News"),
        }
        repo.update_enrichment(company_id, fields)
        updated += 1
    return updated



