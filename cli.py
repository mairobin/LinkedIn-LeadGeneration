import argparse
import json
from pathlib import Path
from typing import List

import logging
from db.connection import get_connection
from db import schema
from pipelines.ingest_profiles import ingest_profiles
from pipelines.enrich_companies import enrich_batch
from services.domain_utils import normalize_linkedin_profile_url
from services.enrichment_service import fetch_company_enrichment, fetch_company_enrichment_linkup
from sqlite_storage import SQLiteStorage
from config.settings import get_settings
from utils.logging_setup import init_logging


def cmd_bootstrap(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	print("Schema ready")


def cmd_ingest(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	data = json.loads(Path(args.input).read_text(encoding="utf-8"))
	profiles = data.get("profiles") or data
	count = ingest_profiles(conn, profiles)
	print(f"Ingested {count} profiles")


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
    fetcher = _stub_fetcher
    if getattr(args, "use_linkup", False):
        def _linkup_fetch(name: str, domain: str):
            data = fetch_company_enrichment_linkup(name, domain)
            return data if data else _stub_fetcher(name, domain)
        fetcher = _linkup_fetch
    elif getattr(args, "use_ai", False):
        def _ai_fetch(name: str, domain: str):
            data = fetch_company_enrichment(name, domain)
            return data if data else _stub_fetcher(name, domain)
        fetcher = _ai_fetch
    def _progress(cur, total, company_id, name):
        print(f"[{cur}/{total}] Enriching company_id={company_id} name={name}")
    updated = enrich_batch(conn, fetcher, limit=getattr(args, "limit", 50), on_progress=_progress if getattr(args, "progress", False) else None)
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


def cmd_snapshot(args):
	# Persist full Person-schema JSON into snapshot table `profiles`
	data = json.loads(Path(args.input).read_text(encoding="utf-8"))
	profiles = data.get("profiles") or data
	# If data appears in raw format (no LinkedIn_Profile keys), map to Person schema
	if isinstance(profiles, list) and profiles and not any("LinkedIn_Profile" in p for p in profiles if isinstance(p, dict)):
		try:
			from datetime import datetime as _dt
			from main import map_to_person_schema as _map
			lookup_date = _dt.utcnow().strftime('%Y-%m-%d')
			profiles = _map(profiles, lookup_date)
		except Exception:
			pass
	store = SQLiteStorage(args.db)
	try:
		count = store.upsert_profiles(profiles)
		print(f"Snapshotted {count} profiles to '{store.table_name}'")
	finally:
		store.close()

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

	p_ing = sub.add_parser("ingest", help="Ingest profiles JSON into normalized tables")
	p_ing.add_argument("--input", required=True, help="Path to JSON file (profiles or array)")
	p_ing.set_defaults(func=cmd_ingest)

	p_snap = sub.add_parser("snapshot", help="Write full Person-schema JSON into 'profiles' snapshot table")
	p_snap.add_argument("--input", required=True, help="Path to JSON file (profiles or array)")
	p_snap.set_defaults(func=cmd_snapshot)

	p_enr = sub.add_parser("enrich", help="Run enrichment batch (stub by default; --use-ai OpenAI or --use-linkup)")
	p_enr.add_argument("--use-ai", action="store_true", help="Use OpenAI to fetch enrichment; falls back to stub if unavailable")
	p_enr.add_argument("--use-linkup", action="store_true", help="Use Linkup to fetch enrichment; falls back to stub if unavailable")
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

	args = parser.parse_args()
	args.func(args)


if __name__ == "__main__":
	main()
