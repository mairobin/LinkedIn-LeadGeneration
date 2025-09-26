from __future__ import annotations

import re
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from db.repos.companies_repo import CompaniesRepo
from pipelines.runner import RunContext
from services.domain_utils import extract_apex_domain


def _canonicalize_legal_form(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip().strip("\"'")
    text = re.sub(r"\s*\([^)]*\)\s*", " ", text).strip()
    low = text.lower()

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
    normalized = re.sub(r"\s+", " ", low.replace(",", " ")).strip()
    normalized = normalized.replace("& co kg", "& co. kg")
    if normalized in shorts:
        return shorts[normalized]

    token = re.sub(r"[^a-z.]", "", low)
    if token in shorts:
        return shorts[token]
    return text if text else None


def _extract_legal_form_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = name.strip()
    if not n:
        return None
    low = n.lower()

    compound_patterns = [
        (r"\bgmbh\s*&\s*co\.?\s*kg\b", "GmbH & Co. KG"),
        (r"\bag\s*&\s*co\.?\s*kg\b", "AG & Co. KG"),
        (r"\bse\s*&\s*co\.?\s*kg\b", "SE & Co. KG"),
        (r"\bkgaa\b", "KGaA"),
    ]
    for pattern, canonical in compound_patterns:
        if re.search(pattern, low):
            return canonical

    simple_suffixes = [
        (r"\bgmbh\b", "GmbH"),
        (r"\bag\b", "AG"),
        (r"\bse\b", "SE"),
        (r"\bug\b", "UG"),
        (r"\bohg\b", "OHG"),
        (r"\bkg\b", "KG"),
        (r"\be\.k\.\?\b", "e.K."),
    ]
    for pattern, canonical in simple_suffixes:
        if re.search(pattern, low):
            return canonical
    return None


def _derive_legal_form(company_name: Optional[str], provided: Optional[str]) -> Optional[str]:
    from_name = _extract_legal_form_from_name(company_name)
    if from_name:
        return from_name
    return _canonicalize_legal_form(provided)


class LoadPendingCompanies:
    def __init__(self, conn: sqlite3.Connection, limit: int = 50) -> None:
        self.conn = conn
        self.limit = limit

    def run(self, ctx: RunContext) -> RunContext:
        repo = CompaniesRepo(self.conn)
        rows = repo.get_pending_companies(limit=self.limit)
        companies: List[Dict[str, Any]] = []
        for (company_id, name, domain) in rows:
            companies.append({"id": company_id, "name": name, "domain": domain})
        ctx.companies = companies
        ctx.meta["pending_companies_total"] = len(companies)
        return ctx


class EnrichAndPersistCompanies:
    def __init__(self, conn: sqlite3.Connection, fetch_func: Callable[[Optional[str], Optional[str]], Optional[Dict[str, Any]]], on_progress: Optional[Callable[[int, int, int, str], None]] = None) -> None:
        self.conn = conn
        self.fetch_func = fetch_func
        self.on_progress = on_progress

    def run(self, ctx: RunContext) -> RunContext:
        repo = CompaniesRepo(self.conn)
        companies = ctx.companies or []
        total = len(companies)
        updated = 0
        for idx, comp in enumerate(companies, start=1):
            company_id = comp.get("id")
            name = comp.get("name")
            domain = comp.get("domain")
            if self.on_progress:
                try:
                    self.on_progress(idx, total, int(company_id), str(name or ""))
                except Exception:
                    pass
            data = self.fetch_func(name, domain)
            if not isinstance(data, dict):
                continue
            legal_form = _derive_legal_form(name, data.get("Legal_Form"))
            website_val = data.get("Website")
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
            repo.save_company_enrichment(int(company_id), fields)
            updated += 1
        ctx.meta["companies_enriched"] = updated
        return ctx


