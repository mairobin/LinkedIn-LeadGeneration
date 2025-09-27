from __future__ import annotations

import sqlite3
from typing import Callable, Optional

from db.repos.people_repo import PeopleRepo
from db.repos.companies_repo import CompaniesRepo
from db.repos.queries_repo import QueriesRepo
from services.domain_utils import extract_apex_domain, normalize_linkedin_profile_url
from pipelines.runner import RunContext
import re


class PersistPeople:
    def __init__(self, conn: sqlite3.Connection, on_processed: Optional[Callable[[int], None]] = None) -> None:
        self.conn = conn
        self.people_repo = PeopleRepo(conn)
        self.companies_repo = CompaniesRepo(conn)
        self.queries_repo = QueriesRepo(conn)
        self.on_processed = on_processed

    @staticmethod
    def _extract_degree_prefix(full_name: Optional[str]) -> tuple[Optional[str], str]:
        """Extract common German academic/ professional degree prefixes from the start of a name.

        Returns (degree, cleaned_name). Degree excludes trailing space and is None if not found.
        """
        text = (full_name or "").strip()
        if not text:
            return None, ""
        # Common degree prefixes in Germany (expandable). Ordered for longest-first matching.
        patterns = [
            r"^prof\.?\s+dr\.?\s+",           # Prof. Dr. 
            r"^dr\.-ing\.?\s+",               # Dr.-Ing.
            r"^dipl\.-ing\.?\s+",             # Dipl.-Ing.
            r"^priv\.-doz\.?\s+",             # Priv.-Doz.
            r"^prof\.?\s+",                    # Prof.
            r"^pd\.?\s+",                      # PD.
            r"^dr\.?\s+",                      # Dr.
            r"^mag\.?\s+",                     # Mag.
            r"^ing\.?\s+",                     # Ing.
            r"^mba\s+",                         # MBA
            r"^m\.sc\.?\s+",                  # M.Sc.
            r"^b\.sc\.?\s+",                  # B.Sc.
        ]
        for pat in patterns:
            m = re.match(pat, text, flags=re.IGNORECASE)
            if m:
                deg = text[:m.end()].strip()
                # Normalize spacing/casing of degree as it appeared
                cleaned = text[m.end():].strip()
                # Ensure standard dot casing e.g., Dr. vs dr.
                degree_norm = deg.strip()
                return degree_norm, cleaned
        return None, text

    def run(self, ctx: RunContext) -> RunContext:
        processed = 0
        # Resolve canonical query id once per run (if provided in records)
        canonical_query_id = None
        try:
            # Prefer consistent values taken from the first record that has both name and query
            for peek in (ctx.people or []):
                src_name = peek.get('source_name') if isinstance(peek, dict) else None
                src_query = peek.get('source_query') if isinstance(peek, dict) else None
                if src_name and src_query:
                    canonical_query_id = self.queries_repo.find_or_create(src_name, 'person', src_query)
                    break
        except Exception:
            canonical_query_id = None

        for p in (ctx.people or []):
            profile_url = normalize_linkedin_profile_url(p.get('LinkedIn_Profile') or p.get('profile_url'))
            if not profile_url:
                continue

            def _to_int(value):
                try:
                    return int(value) if value is not None else None
                except Exception:
                    return None

            raw_name = (p.get('Contact_Name') or p.get('name') or '').strip()
            degree_prefix, cleaned_name = self._extract_degree_prefix(raw_name)
            first = cleaned_name.split(' ')[0] or None if cleaned_name else None
            last = None
            try:
                parts = [s for s in str(cleaned_name).split(' ') if s]
                if len(parts) >= 2:
                    first, last = parts[0], ' '.join(parts[1:])
            except Exception:
                pass
            title = p.get('Position') or p.get('current_position')
            if degree_prefix:
                if title and degree_prefix:
                    title = f"{title} ({degree_prefix})"
                elif degree_prefix:
                    title = f"({degree_prefix})"
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
                search_query_id=canonical_query_id,
            )

            company_name = p.get('Company') or p.get('company')
            website = p.get('Company_Website') or p.get('company_website') or p.get('Website_Info') or p.get('website')
            domain = p.get('Company_Domain') or extract_apex_domain(website)
            company_id = None
            if domain or company_name:
                company_id = self.companies_repo.upsert_company(company_name, domain, website, source_name=source_name, source_query=source_query, search_query_id=canonical_query_id)

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


