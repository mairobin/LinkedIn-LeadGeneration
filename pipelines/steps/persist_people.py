from __future__ import annotations

import sqlite3
from typing import Callable, Optional

from db.repos.people_repo import PeopleRepo
from db.repos.companies_repo import CompaniesRepo
from services.domain_utils import extract_apex_domain, normalize_linkedin_profile_url
from pipelines.runner import RunContext


class PersistPeople:
    def __init__(self, conn: sqlite3.Connection, on_processed: Optional[Callable[[int], None]] = None) -> None:
        self.conn = conn
        self.people_repo = PeopleRepo(conn)
        self.companies_repo = CompaniesRepo(conn)
        self.on_processed = on_processed

    def run(self, ctx: RunContext) -> RunContext:
        processed = 0
        for p in (ctx.people or []):
            profile_url = normalize_linkedin_profile_url(p.get('LinkedIn_Profile') or p.get('profile_url'))
            if not profile_url:
                continue

            def _to_int(value):
                try:
                    return int(value) if value is not None else None
                except Exception:
                    return None

            first = (p.get('Contact_Name') or p.get('name') or '').split(' ')[0] or None
            last = None
            try:
                parts = [s for s in str(p.get('Contact_Name') or p.get('name') or '').split(' ') if s]
                if len(parts) >= 2:
                    first, last = parts[0], ' '.join(parts[1:])
            except Exception:
                pass
            title = p.get('Position') or p.get('current_position')
            email = p.get('Email') or p.get('email')
            location = p.get('Location') or p.get('location')

            connections = p.get('Connections_LinkedIn') or p.get('connection_count')
            followers = p.get('Followers_LinkedIn') or p.get('follower_count')
            try:
                from utils.number_parsing import _parse_connections, _parse_int_shorthand  # type: ignore
                connections_val = _parse_connections(connections)
                followers_val = _parse_int_shorthand(followers)
            except Exception:
                connections_val = _to_int(connections)
                followers_val = _to_int(followers)

            website_info = p.get('Website_Info') or p.get('company_website') or p.get('website')
            phone_info = p.get('Phone_Info') or p.get('phone')
            info_raw = p.get('Info_raw') or p.get('summary')
            insights_val = p.get('Insights') or p.get('summary_other')
            if isinstance(insights_val, list):
                insights_text = '; '.join([str(x) for x in insights_val if x])
            elif isinstance(insights_val, str):
                insights_text = insights_val
            else:
                insights_text = None
            lookup_date = p.get('Lookup_Date')

            source_name = p.get('source_name') or None
            source_query = p.get('source_query') or None

            person_id = self.people_repo.upsert(
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

            company_name = p.get('Company') or p.get('company')
            website = p.get('Company_Website') or p.get('company_website') or p.get('Website_Info') or p.get('website')
            domain = p.get('Company_Domain') or extract_apex_domain(website)
            company_id = None
            if domain or company_name:
                company_id = self.companies_repo.upsert_company(company_name, domain, website, source_name=source_name, source_query=source_query)

            if company_id:
                self.people_repo.link_person_to_company(profile_url, company_id)

            processed += 1
            if self.on_processed:
                try:
                    self.on_processed(processed)
                except Exception:
                    pass

        ctx.meta['processed_people'] = processed
        return ctx


