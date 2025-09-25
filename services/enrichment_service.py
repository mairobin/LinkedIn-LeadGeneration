from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from config.settings import get_settings
from services.domain_utils import extract_apex_domain
"""All env loading is centralized in config.settings; no direct dotenv here."""


PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "enrichment_prompt.txt"


def _load_prompt_template() -> str:
    try:
        return PROMPT_PATH.read_text(encoding="utf-8")
    except Exception:
        # Minimal inline fallback prompt if file missing
        return (
            "Return ONLY JSON with keys: Company, Legal_Form, Industries, Locations_Germany, "
            "Multinational, Website, Size_Employees, Business_Model_Key_Points, "
            "Products_and_Services, Recent_News. Use null when unknown."
        )


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    # Try raw parse first
    try:
        return json.loads(text)
    except Exception:
        pass
    # Try fenced blocks
    try:
        import re
        m = re.search(r"```(?:json)?\n([\s\S]*?)\n```", text)
        if m:
            return json.loads(m.group(1))
    except Exception:
        pass
    # Try to find first { ... } block
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
    except Exception:
        pass
    return None


def _google_search_homepage(company_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Use Google CSE to find a likely homepage URL for the company.

    Returns (homepage_url, apex_domain).
    """
    settings = get_settings()
    if not settings.google_api_key or not settings.google_cse_id or not company_name:
        return None, None
    try:
        params = {
            "key": settings.google_api_key,
            "cx": settings.google_cse_id,
            "q": company_name,
            "num": 5,
        }
        resp = requests.get(settings.google_search_url, params=params, timeout=settings.request_timeout_seconds)
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        items = data.get("items") or []
        for item in items:
            link = item.get("link")
            display_link = (item.get("displayLink") or "").lower()
            if not link:
                continue
            # Skip social/linkedin results
            if any(bad in display_link for bad in ["linkedin.com", "twitter.com", "facebook.com", "instagram.com"]):
                continue
            apex = extract_apex_domain(link)
            if apex:
                return link, apex
        return None, None
    except Exception:
        return None, None


def _fetch_page_text(url: str, max_chars: int = 4000) -> Optional[str]:
    try:
        settings = get_settings()
        resp = requests.get(url, timeout=settings.request_timeout_seconds, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Drop script/style
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        text = " ".join((soup.get_text(" ") or "").split())
        return text[:max_chars]
    except Exception:
        return None


def fetch_company_enrichment(company_name: str, domain: Optional[str]) -> Optional[Dict[str, Any]]:
    """Call OpenAI to fetch enrichment data adhering to the prompt JSON contract.

    Returns a dict on success, or None if unavailable/failure.
    """
    settings = get_settings()
    api_key = settings.openai_api_key
    if not api_key:
        return None

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        prompt = _load_prompt_template()
        homepage_url = None
        apex = domain
        if not apex:
            homepage_url, apex = _google_search_homepage(company_name)
        if homepage_url and not apex:
            apex = extract_apex_domain(homepage_url)
        page_text = _fetch_page_text(homepage_url) if homepage_url else None

        target = f"Target company: {company_name or ''}. Domain: {apex or domain or 'unknown'}"
        context = ""
        if homepage_url and page_text:
            context = f"\nWebsite URL: {homepage_url}\nWebsite excerpt (truncated):\n{page_text}\n"
        user_msg = f"{prompt}\n\n{target}{context}"

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a precise analyst. Output only valid JSON when asked."},
                {"role": "user", "content": user_msg},
            ],
        )
        content = resp.choices[0].message.content if resp.choices else None
        data = _extract_json(content or "")
        return data
    except Exception:
        return None


def fetch_company_enrichment_linkup(company_name: str, domain: Optional[str]) -> Optional[Dict[str, Any]]:
    """Use Linkup to perform structured web-backed research according to the schema.

    Requires LINKUP_API_KEY (or pass api_key during client init).
    """
    try:
        from linkup import LinkupClient
        from pydantic import BaseModel
    except Exception:
        return None

    settings = get_settings()
    api_key = settings.linkup_api_key
    if not api_key:
        # Allow caller to configure env; if not present, return None
        return None

    class CompanyResearch(BaseModel):
        Company: str
        Legal_Form: Optional[str]
        Industries: list[str]
        Locations_Germany: list[str]
        Multinational: bool
        Website: Optional[str]
        Size_Employees: Optional[int]
        Business_Model_Key_Points: list[str]
        Products_and_Services: list[str]
        Recent_News: list[str]

    client = LinkupClient(api_key=api_key)

    prompt = _load_prompt_template()
    target = f"\n\nTarget company: {company_name or ''}. Domain: {domain or 'unknown'}"
    query = f"{prompt}{target}"

    try:
        resp = client.search(
            query=query,
            depth="standard",
            output_type="structured",
            structured_output_schema=CompanyResearch,
        )
        # Linkup client may already return a dict that matches the model; ensure dict
        if hasattr(resp, "model_dump"):
            return resp.model_dump()
        if isinstance(resp, dict):
            return resp
        # Fallback: try to coerce through pydantic
        try:
            parsed = CompanyResearch.model_validate(resp)
            return parsed.model_dump()
        except Exception:
            return None
    except Exception:
        return None


