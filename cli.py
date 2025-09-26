import argparse
import json
from pathlib import Path
from typing import List

import logging
from db.connection import get_connection
from db import schema
from pipelines.runner import Pipeline, RunContext
from pipelines.steps.enrich_companies import LoadPendingCompanies, EnrichAndPersistCompanies
from pipelines.steps.validate_companies import ValidateCompanies
from pipelines.steps.persist_companies import PersistCompanies
from pipelines.steps.validate_people import ValidatePeople
from pipelines.steps.persist_people import PersistPeople
from services.domain_utils import normalize_linkedin_profile_url
from services.mapping import map_to_person_schema
from services.reporting import print_summary
from services.enrichment_service import fetch_company_enrichment, fetch_company_enrichment_linkup
from config.settings import get_settings
from utils.logging_setup import init_logging
from pipelines.steps.validate_data import DataValidator
from sources.registry import get_source
import os
from datetime import datetime, timezone
import uuid as _uuid


def cmd_bootstrap(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	print("Schema ready")


def cmd_ingest(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	data = json.loads(Path(args.input).read_text(encoding="utf-8"))
	profiles = data.get("profiles") or data
	# Build and run people ingestion pipeline directly
	ctx = RunContext()
	ctx.people = list(profiles)
	pipeline = Pipeline([
		ValidatePeople(),
		PersistPeople(conn),
	])
	ctx = pipeline.run(ctx)
	count = int(ctx.meta.get('processed_people') or 0)
	print(f"Ingested {count} people")


def _stub_fetcher(name: str, domain: str):
	# Placeholder enrichment fetcher; replace with OpenAI/web search logic
	return {
		"Company": name,
		"Legal_Form": None,
		"Industries": [],
		"Locations_Germany": [],
		"Multinational": False,
		"Website": f"https://{domain}" if domain else None,
		"Size_Employees": None,
		"Business_Model_Key_Points": [],
		"Products_and_Services": [],
		"Recent_News": [],
	}


def cmd_enrich(args):
    conn = get_connection(args.db)
    schema.bootstrap(conn)
    settings = get_settings()
    provider = (settings.ai_provider or "stub").lower()

    if provider in ("openai", "linkup"):
        def _gateway_fetch(name: str, domain: str):
            # Central gateway handles provider routing (OpenAI or Linkup) per config routes
            data = fetch_company_enrichment(name, domain)
            return data if data else _stub_fetcher(name, domain)
        fetcher = _gateway_fetch
    else:
        # Stub provider allowed only in test environment
        if (settings.run_env or "").lower() != "test":
            raise RuntimeError("Stub enrichment provider is only allowed when RUN_ENV=test")
        fetcher = _stub_fetcher

    def _progress(cur, total, company_id, name):
        print(f"[{cur}/{total}] Enriching company_id={company_id} name={name}")

    ctx = RunContext()
    pipeline = Pipeline([
        LoadPendingCompanies(conn, limit=getattr(args, "limit", 50)),
        EnrichAndPersistCompanies(conn, fetcher, on_progress=_progress if getattr(args, "progress", False) else None),
    ])
    ctx = pipeline.run(ctx)
    updated = int(ctx.meta.get("companies_enriched") or 0)
    print(f"Enriched {updated} companies")

def cmd_report_person(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	profile = normalize_linkedin_profile_url(args.profile)
	if not profile:
		print("Invalid LinkedIn profile URL")
		return
	sql = (
		"SELECT person_id, first_name, last_name, linkedin_profile, title_current, email, location_text, "
		"       connections_linkedin, followers_linkedin, "
		"       company_id, company_name, domain, website, size_employees, legal_form, industries_json, "
		"       locations_de_json, multinational, strftime('%Y-%m-%d %H:%M', datetime(last_enriched_at, 'localtime')) AS last_enriched "
		"FROM v_people_with_company WHERE linkedin_profile = ?"
	)
	cur = conn.cursor()
	cur.execute(sql, (profile,))
	row = cur.fetchone()
	if not row:
		print("No record found for profile")
		return
	keys = [
		"person_id","first_name","last_name","linkedin_profile","title_current","email","location_text",
		"connections_linkedin","followers_linkedin",
		"company_id","company_name","domain","website","size_employees","legal_form","industries_json",
		"locations_de_json","multinational","last_enriched"
	]
	result = {k: row[i] for i, k in enumerate(keys)}
	import json
	print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_run(args):
	settings = get_settings()
	if not os.getenv("RUN_ID"):
		try:
			os.environ["RUN_ID"] = _uuid.uuid4().hex
		except Exception:
			pass
	if args.pipeline == "ingest-people":
		# Determine search terms
		if args.query:
			search_terms = args.query.split()
		else:
			search_terms = args.terms
		# Resolve sources
		src_names = args.source or ["linkedin_people_google"]
		all_people = []
		all_companies = []
		for src_name in src_names:
			src = get_source(src_name)
			data = src.run(search_terms, args.max_results)
			if data:
				for rec in data:
					try:
						rec['source_name'] = getattr(src, 'source_name', src_name)
						rec['source_query'] = ' '.join(search_terms)
					except Exception:
						pass
			if getattr(src, 'entity_type', None) == 'person':
				all_people.extend(data or [])
			elif getattr(src, 'entity_type', None) == 'company':
				all_companies.extend(data or [])
		# Map and validate
		validator = DataValidator()
		lookup_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
		transformed_profiles = []
		if all_people:
			transformed_profiles = map_to_person_schema(all_people, lookup_date)
			for src_rec, mapped in zip(all_people, transformed_profiles):
				if isinstance(src_rec, dict) and isinstance(mapped, dict):
					if 'source_name' in src_rec:
						mapped['source_name'] = src_rec.get('source_name')
					if 'source_query' in src_rec:
						mapped['source_query'] = src_rec.get('source_query')
		api_usage = { 'api_calls_made': 0, 'estimated_daily_limit_used': 'N/A' }
		output_data = validator.format_output_structure(
			transformed_profiles,
			{ 'query': ' '.join(search_terms), 'search_terms': search_terms },
			{},
			api_usage,
			None
		)
		print_summary(output_data, api_usage)
		if args.write_db:
			conn = get_connection(args.db)
			schema.bootstrap(conn)
			processed_people = 0
			processed_companies = 0
			if transformed_profiles:
				# Run people ingestion pipeline directly (replaces legacy ingest_profiles)
				ctx = RunContext()
				ctx.people = list(transformed_profiles)
				pipeline = Pipeline([
					ValidatePeople(),
					PersistPeople(conn),
				])
				ctx = pipeline.run(ctx)
				processed_people = int(ctx.meta.get('processed_people') or 0)
			if all_companies:
				# Run company ingestion pipeline
				cctx = RunContext()
				cctx.companies = list(all_companies)
				cpipeline = Pipeline([
					ValidateCompanies(),
					PersistCompanies(conn),
				])
				cctx = cpipeline.run(cctx)
				processed_companies = int(cctx.meta.get('processed_companies') or 0)
			print(f"DB write complete: people={processed_people}, companies={processed_companies}")
		return
	elif args.pipeline == "enrich-companies":
		# Reuse enrichment command path
		ns = argparse.Namespace()
		setattr(ns, 'db', args.db)
		setattr(ns, 'limit', args.limit)
		setattr(ns, 'progress', args.progress)
		cmd_enrich(ns)
		return

def cmd_report_recent(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	where = []
	params = []
	if args.min_connections is not None:
		where.append("p.connections_linkedin >= ?")
		params.append(args.min_connections)
	if args.min_followers is not None:
		where.append("p.followers_linkedin >= ?")
		params.append(args.min_followers)
	where_sql = (" WHERE " + " AND ".join(where)) if where else ""
	order_sql = "p.rowid DESC"
	if args.sort_by == "connections":
		order_sql = "p.connections_linkedin DESC NULLS LAST, p.rowid DESC"
	elif args.sort_by == "followers":
		order_sql = "p.followers_linkedin DESC NULLS LAST, p.rowid DESC"
	sql = (
		"SELECT p.id AS person_id, p.first_name, p.last_name, p.linkedin_profile, p.title_current, "
		"       p.connections_linkedin, p.followers_linkedin, "
		"       c.id AS company_id, c.name AS company_name, c.domain "
		f"FROM people p LEFT JOIN companies c ON p.company_id = c.id{where_sql} "
		f"ORDER BY {order_sql} LIMIT ?"
	)
	cur = conn.cursor()
	cur.execute(sql, (*params, args.limit))
	rows = cur.fetchall()
	import json
	out = []
	for r in rows:
		out.append({
			"person_id": r[0],
			"first_name": r[1],
			"last_name": r[2],
			"linkedin_profile": r[3],
			"title_current": r[4],
			"connections_linkedin": r[5],
			"followers_linkedin": r[6],
			"company_id": r[7],
			"company_name": r[8],
			"domain": r[9],
		})
	print(json.dumps(out, indent=2, ensure_ascii=False))


def cmd_dedupe_people(args):
    conn = get_connection(args.db)
    schema.bootstrap(conn)
    cur = conn.cursor()
    # Find duplicates by canonical linkedin_profile after applying normalization again
    # Build a temporary mapping from raw to normalized and merge
    # Note: people.linkedin_profile is UNIQUE, so duplicates will be variants elsewhere (e.g., prior bad normalization or different slugs differing only in case/encoding)
    from services.domain_utils import normalize_linkedin_profile_url as _norm
    cur.execute("SELECT id, linkedin_profile FROM people")
    rows = cur.fetchall()
    norm_to_ids = {}
    for pid, url in rows:
        norm = _norm(url)
        if not norm:
            norm = url
        norm_to_ids.setdefault(norm, []).append((pid, url))
    merged = 0
    for norm_url, entries in norm_to_ids.items():
        if len(entries) <= 1:
            continue
        # Keep the smallest id as primary
        entries.sort(key=lambda x: x[0])
        primary_id, primary_url = entries[0]
        dup_entries = entries[1:]
        # For each duplicate, merge fields into primary, update references, then delete duplicate
        for did, dup_url in dup_entries:
            # Merge non-null person fields into primary where primary is null
            cur.execute("SELECT first_name, last_name, title_current, email, location_text, connections_linkedin, followers_linkedin, website_info, phone_info, info_raw, insights_text, lookup_date, is_hot, status, notes, last_interaction_date, company_id FROM people WHERE id = ?", (did,))
            r = cur.fetchone()
            cols = [
                'first_name','last_name','title_current','email','location_text','connections_linkedin','followers_linkedin',
                'website_info','phone_info','info_raw','insights_text','lookup_date','is_hot','status','notes','last_interaction_date','company_id'
            ]
            updates = []
            params = []
            for i, col in enumerate(cols):
                val = r[i]
                if val is not None and val != '':
                    updates.append(f"{col} = COALESCE({col}, ?)")
                    params.append(val)
            if updates:
                cur.execute(f"UPDATE people SET {', '.join(updates)} WHERE id = ?", (*params, primary_id))
            # Update outreach messages to reference the normalized URL (or primary URL for now)
            try:
                cur.execute("UPDATE outreach_messages SET linkedin_profile = ? WHERE linkedin_profile = ?", (norm_url, dup_url))
            except Exception:
                pass
            # Delete duplicate row now to avoid UNIQUE conflicts
            cur.execute("DELETE FROM people WHERE id = ?", (did,))
            merged += 1
        # Finally, set the primary linkedin_profile to the normalized canonical URL if different
        cur.execute("SELECT linkedin_profile FROM people WHERE id = ?", (primary_id,))
        current = cur.fetchone()[0]
        if current != norm_url:
            cur.execute("UPDATE people SET linkedin_profile = ? WHERE id = ?", (norm_url, primary_id))
    conn.commit()
    print(f"Merged {merged} duplicate person rows")

def main():
    settings = get_settings()
    init_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Lead DB CLI")
    parser.add_argument("--db", default=settings.db_path, help="Path to SQLite DB (default from settings)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_boot = sub.add_parser("bootstrap", help="Create tables and indexes")
    p_boot.set_defaults(func=cmd_bootstrap)

    p_ing = sub.add_parser("ingest", help="Ingest people JSON into normalized tables")
    p_ing.add_argument("--input", required=True, help="Path to JSON file (array or object)")
    p_ing.set_defaults(func=cmd_ingest)

    p_enr = sub.add_parser("enrich", help="Run enrichment batch (provider from settings.ai_provider)")
    p_enr.add_argument("--limit", type=int, default=10, help="Max companies to enrich in this run (default: 10)")
    p_enr.add_argument("--progress", action="store_true", help="Print progress for each company")
    p_enr.set_defaults(func=cmd_enrich)

    p_rp = sub.add_parser("report-person", help="Show joined person+company for a LinkedIn profile")
    p_rp.add_argument("--profile", required=True, help="LinkedIn profile URL")
    p_rp.set_defaults(func=cmd_report_person)

    p_rr = sub.add_parser("report-recent", help="List recent people with company context")
    p_rr.add_argument("--limit", type=int, default=5)
    p_rr.add_argument("--sort-by", choices=["recent","connections","followers"], default="recent", help="Sort by recent (default), connections or followers")
    p_rr.add_argument("--min-connections", type=int, default=None, help="Filter: minimum LinkedIn connections")
    p_rr.add_argument("--min-followers", type=int, default=None, help="Filter: minimum LinkedIn followers")
    p_rr.set_defaults(func=cmd_report_recent)

    p_dd = sub.add_parser("dedupe-people", help="Merge duplicate people rows by canonical LinkedIn URL")
    p_dd.set_defaults(func=cmd_dedupe_people)

    # Unified runner
    p_run = sub.add_parser("run", help="Run a named pipeline")
    p_run.add_argument("pipeline", choices=["ingest-people", "enrich-companies"], help="Pipeline to run")
    # Ingest flags
    p_run.add_argument('--query', '-q', type=str, help='Search query as a single string')
    qg = p_run.add_mutually_exclusive_group(required=False)
    qg.add_argument('--terms', '-t', nargs='+', help='Search terms as separate arguments')
    p_run.add_argument('--source', '-s', action='append', help='Source name to run (repeatable). Default: linkedin_people_google')
    p_run.add_argument('--max-results', '-m', type=int, default=settings.default_max_results, help='Maximum results to collect')
    p_run.add_argument('--write-db', action='store_true', help='Write normalized people/companies to SQLite')
    # Enrichment flags
    p_run.add_argument('--limit', type=int, default=10, help='Max companies to enrich (default: 10)')
    p_run.add_argument('--progress', action='store_true', help='Print progress for each company')
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
