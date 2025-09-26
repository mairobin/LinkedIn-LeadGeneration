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

        # Parse counts if present
        def _to_int(value):
            try:
                return int(value) if value is not None else None
            except Exception:
                return None

        connections = p.get('Connections_LinkedIn') or p.get('connection_count')
        followers = p.get('Followers_LinkedIn') or p.get('follower_count')
        try:
            from utils.number_parsing import _parse_connections, _parse_int_shorthand  # type: ignore
            connections_val = _parse_connections(connections)
            followers_val = _parse_int_shorthand(followers)
        except Exception:
            connections_val = _to_int(connections)
            followers_val = _to_int(followers)

        # Additional personal fields
        website_info = p.get('Website_Info') or p.get('company_website') or p.get('website')
        phone_info = p.get('Phone_Info') or p.get('phone')
        info_raw = p.get('Info_raw') or p.get('summary')
        insights_val = p.get('Insights') or p.get('summary_other')
        if isinstance(insights_val, list):
            insights_text = "; ".join([str(x) for x in insights_val if x])
        elif isinstance(insights_val, str):
            insights_text = insights_val
        else:
            insights_text = None
        lookup_date = p.get('Lookup_Date')

        # Capture provenance if present on the transformed profile
        source_name = p.get('source_name') or None
        source_query = p.get('source_query') or None

        person_id = people.upsert_person(
            linkedin_profile=profile_url,
            first_name=first,
            last_name=last,
            title_current=title,
            email=email,
            location_text=location,
            connections_linkedin=connections_val,
            followers_linkedin=followers_val,
            website_info=website_info,
            phone_info=phone_info,
            info_raw=info_raw,
            insights_text=insights_text,
            lookup_date=lookup_date,
            source_name=source_name,
            source_query=source_query,
        )

        # Determine apex domain
        company_name = p.get('Company') or p.get('company')
        website = p.get('Company_Website') or p.get('company_website') or p.get('Website_Info') or p.get('website')
        domain = p.get('Company_Domain') or extract_apex_domain(website)

        company_id = None
        if domain or company_name:
            company_id = companies.upsert_by_domain(company_name, domain, website, source_name=source_name, source_query=source_query)

        if company_id:
            people.link_person_to_company(profile_url, company_id)

        processed += 1

    return processed




