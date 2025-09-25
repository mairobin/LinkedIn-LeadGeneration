from __future__ import annotations

from typing import Dict, Iterable, Optional, Tuple
import sqlite3

from db.repos.people_repo import PeopleRepo
from db.repos.companies_repo import CompaniesRepo
from services.domain_utils import extract_apex_domain, normalize_linkedin_profile_url


def split_name(full_name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not full_name:
        return None, None
    parts = [p for p in str(full_name).strip().split() if p]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def ingest_profiles(conn: sqlite3.Connection, profiles: Iterable[Dict]) -> int:
    """Ingest extracted profiles into normalized people+companies and link by domain.

    Returns count of processed profiles.
    """
    people = PeopleRepo(conn)
    companies = CompaniesRepo(conn)
    processed = 0

    for p in profiles:
        profile_url = normalize_linkedin_profile_url(p.get('LinkedIn_Profile') or p.get('profile_url'))
        if not profile_url:
            continue

        first, last = split_name(p.get('Contact_Name') or p.get('name'))
        title = p.get('Position') or p.get('current_position')
        email = p.get('Email') or p.get('email')
        location = p.get('Location') or p.get('location')

        person_id = people.upsert_person(
            linkedin_profile=profile_url,
            first_name=first,
            last_name=last,
            title_current=title,
            email=email,
            location_text=location,
        )

        # Determine apex domain
        company_name = p.get('Company') or p.get('company')
        website = p.get('Company_Website') or p.get('company_website') or p.get('Website_Info') or p.get('website')
        domain = p.get('Company_Domain') or extract_apex_domain(website)

        company_id = None
        if domain or company_name:
            company_id = companies.upsert_by_domain(company_name, domain, website)

        if company_id:
            people.link_person_to_company(profile_url, company_id)

        processed += 1

    return processed



