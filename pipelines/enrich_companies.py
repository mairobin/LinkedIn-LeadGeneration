from __future__ import annotations

import json
from typing import Dict, List, Callable, Optional
import re
import sqlite3

from db.repos.companies_repo import CompaniesRepo
from services.domain_utils import extract_apex_domain


def _canonicalize_legal_form(raw: Optional[str]) -> Optional[str]:
    """Normalize messy legal form strings to concise canonical abbreviations.

    Examples:
      - "GmbH (Germany)" -> "GmbH"
      - "Gesellschaft mit beschränkter Haftung" -> "GmbH"
      - "Aktiengesellschaft" -> "AG"
    """
    if not raw:
        return None
    text = str(raw).strip().strip('"\'')
    # Drop parenthetical/country notes
    text = re.sub(r"\s*\([^)]*\)\s*", " ", text).strip()
    low = text.lower()

    # Phrase mappings (long -> short)
    phrase_map = {
        "gesellschaft mit beschränkter haftung": "GmbH",
        "beschränkter haftung": "GmbH",
        "aktiengesellschaft": "AG",
        "unternehmergesellschaft": "UG",
        "kommanditgesellschaft auf aktien": "KGaA",
        "kommanditgesellschaft": "KG",
        "offene handelsgesellschaft": "OHG",
        "eingetragener kaufmann": "e.K.",
        "eingetragene kauffrau": "e.K.",
        "eingetragene kaufmann": "e.K.",
    }
    for phrase, short in phrase_map.items():
        if phrase in low:
            return short

    # Direct short forms; preserve canonical casing
    shorts = {
        "gmbh": "GmbH",
        "ag": "AG",
        "se": "SE",
        "ug": "UG",
        "kgaa": "KGaA",
        "kg": "KG",
        "ohg": "OHG",
        "e.k.": "e.K.",
        "ek": "e.K.",
        "gmbh & co. kg": "GmbH & Co. KG",
        "ag & co. kg": "AG & Co. KG",
        "se & co. kg": "SE & Co. KG",
    }
    # Normalize punctuation for matching compound strings
    normalized = re.sub(r"\s+", " ", low.replace(",", " ")).strip()
    normalized = normalized.replace("& co kg", "& co. kg")
    if normalized in shorts:
        return shorts[normalized]

    # Try exact token match
    token = re.sub(r"[^a-z.]", "", low)
    if token in shorts:
        return shorts[token]

    # Fallback to cleaned original without parentheses
    return text if text else None


def _extract_legal_form_from_name(name: Optional[str]) -> Optional[str]:
    """Detect legal form from the company name by recognizing common suffixes.

    Priority is given to compound forms like "GmbH & Co. KG".
    """
    if not name:
        return None
    n = name.strip()
    if not n:
        return None
    low = n.lower()

    # Compound forms first (allow optional dots/spaces)
    compound_patterns = [
        (r"\bgmbh\s*&\s*co\.?\s*kg\b", "GmbH & Co. KG"),
        (r"\bag\s*&\s*co\.?\s*kg\b", "AG & Co. KG"),
        (r"\bse\s*&\s*co\.?\s*kg\b", "SE & Co. KG"),
        (r"\bkgaa\b", "KGaA"),
    ]
    for pattern, canonical in compound_patterns:
        if re.search(pattern, low):
            return canonical

    # Simple suffix forms (match at end or as separate token)
    simple_suffixes = [
        (r"\bgmbh\b", "GmbH"),
        (r"\bag\b", "AG"),
        (r"\bse\b", "SE"),
        (r"\bug\b", "UG"),
        (r"\bohg\b", "OHG"),
        (r"\bkg\b", "KG"),
        (r"\be\.k\.?\b", "e.K."),
    ]
    for pattern, canonical in simple_suffixes:
        if re.search(pattern, low):
            return canonical

    return None


def _derive_legal_form(company_name: Optional[str], provided: Optional[str]) -> Optional[str]:
    """Prefer legal form detected in name; otherwise normalize provided value."""
    from_name = _extract_legal_form_from_name(company_name)
    if from_name:
        return from_name
    return _canonicalize_legal_form(provided)


def enrich_batch(
    conn: sqlite3.Connection,
    fetch_func,
    limit: int = 50,
    on_progress: Optional[Callable[[int, int, int, str], None]] = None,
) -> int:
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
    rows = repo.select_pending_enrichment(limit=limit)
    total = len(rows)
    updated = 0
    for idx, (company_id, name, domain) in enumerate(rows, start=1):
        if on_progress:
            try:
                on_progress(idx, total, company_id, name or "")
            except Exception:
                pass
        data = fetch_func(name, domain)
        if not isinstance(data, dict):
            continue
        legal_form = _derive_legal_form(name, data.get("Legal_Form"))
        website_val = data.get("Website")
        # Prefer existing domain; otherwise derive from website
        domain_to_store = domain or (extract_apex_domain(website_val) if website_val else None)
        fields = {
            "legal_form": legal_form,
            "industries_json": data.get("Industries"),
            "locations_de_json": data.get("Locations_Germany"),
            "multinational": 1 if data.get("Multinational") else 0,
            "domain": domain_to_store,
            "website": website_val,
            "size_employees": data.get("Size_Employees"),
            "business_model_json": data.get("Business_Model_Key_Points"),
            "products_json": data.get("Products_and_Services"),
            "recent_news_json": data.get("Recent_News"),
        }
        repo.update_enrichment(company_id, fields)
        updated += 1
    return updated



