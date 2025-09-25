import argparse
import json
from pathlib import Path
from typing import List

from db.connection import get_connection
from db import schema
from pipelines.ingest_profiles import ingest_profiles
from pipelines.enrich_companies import enrich_batch
from services.domain_utils import normalize_linkedin_profile_url


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
	updated = enrich_batch(conn, _stub_fetcher)
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
		"       company_id, company_name, domain, website, size_employees, legal_form, industries_json, "
		"       locations_de_json, multinational, strftime('%Y-%m-%d', last_enriched_at) AS last_enriched "
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
		"company_id","company_name","domain","website","size_employees","legal_form","industries_json",
		"locations_de_json","multinational","last_enriched"
	]
	result = {k: row[i] for i, k in enumerate(keys)}
	import json
	print(json.dumps(result, indent=2, ensure_ascii=False))

def cmd_report_recent(args):
	conn = get_connection(args.db)
	schema.bootstrap(conn)
	sql = (
		"SELECT p.id AS person_id, p.first_name, p.last_name, p.linkedin_profile, p.title_current, "
		"       c.id AS company_id, c.name AS company_name, c.domain "
		"FROM people p LEFT JOIN companies c ON p.company_id = c.id "
		"ORDER BY p.rowid DESC LIMIT ?"
	)
	cur = conn.cursor()
	cur.execute(sql, (args.limit,))
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
			"company_id": r[5],
			"company_name": r[6],
			"domain": r[7],
		})
	print(json.dumps(out, indent=2, ensure_ascii=False))


def main():
	parser = argparse.ArgumentParser(description="Lead DB CLI")
	parser.add_argument("--db", default="leads.db", help="Path to SQLite DB")
	sub = parser.add_subparsers(dest="cmd", required=True)

	p_boot = sub.add_parser("bootstrap", help="Create tables and indexes")
	p_boot.set_defaults(func=cmd_bootstrap)

	p_ing = sub.add_parser("ingest", help="Ingest profiles JSON into normalized tables")
	p_ing.add_argument("--input", required=True, help="Path to JSON file (profiles or array)")
	p_ing.set_defaults(func=cmd_ingest)

	p_enr = sub.add_parser("enrich", help="Run enrichment batch with stub fetcher")
	p_enr.set_defaults(func=cmd_enrich)

	p_rp = sub.add_parser("report-person", help="Show joined person+company for a LinkedIn profile")
	p_rp.add_argument("--profile", required=True, help="LinkedIn profile URL")
	p_rp.set_defaults(func=cmd_report_person)

	p_rr = sub.add_parser("report-recent", help="List recent people with company context")
	p_rr.add_argument("--limit", type=int, default=5)
	p_rr.set_defaults(func=cmd_report_recent)

	args = parser.parse_args()
	args.func(args)


if __name__ == "__main__":
	main()
