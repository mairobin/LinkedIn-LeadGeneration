from __future__ import annotations

import sqlite3


def bootstrap(conn: sqlite3.Connection) -> None:
    """Create normalized schema, indexes, and views (idempotent)."""
    cur = conn.cursor()

    # Companies table
    cur.execute(
        (
            "CREATE TABLE IF NOT EXISTS companies (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  name TEXT,\n"
            "  domain TEXT UNIQUE,\n"
            "  website TEXT,\n"
            "  legal_form TEXT,\n"
            "  industries_json TEXT,\n"
            "  locations_de_json TEXT,\n"
            "  multinational INTEGER,\n"
            "  size_employees INTEGER,\n"
            "  business_model_json TEXT,\n"
            "  products_json TEXT,\n"
            "  recent_news_json TEXT,\n"
            "  last_enriched_at TEXT\n"
            ")" 
        )
    )
    # Helpful index on name lookup
    cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);")

    # People table
    cur.execute(
        (
            "CREATE TABLE IF NOT EXISTS people (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  linkedin_profile TEXT NOT NULL UNIQUE,\n"
            "  first_name TEXT,\n"
            "  last_name TEXT,\n"
            "  title_current TEXT,\n"
            "  email TEXT,\n"
            "  location_text TEXT,\n"
            "  connections_linkedin INTEGER,\n"
            "  followers_linkedin INTEGER,\n"
            "  website_info TEXT,\n"
            "  phone_info TEXT,\n"
            "  info_raw TEXT,\n"
            "  insights_text TEXT,\n"
            "  lookup_date TEXT,\n"
            "  is_hot INTEGER NOT NULL DEFAULT 0,\n"
            "  status TEXT,\n"
            "  notes TEXT,\n"
            "  last_interaction_date TEXT,\n"
            "  company_id INTEGER,\n"
            "  FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL\n"
            ")"
        )
    )
    # Backfill columns if table existed before
    try:
        cur.execute("ALTER TABLE people ADD COLUMN connections_linkedin INTEGER;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN followers_linkedin INTEGER;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN website_info TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN phone_info TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN info_raw TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN insights_text TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN lookup_date TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN is_hot INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN status TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN notes TEXT;")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE people ADD COLUMN last_interaction_date TEXT;")
    except Exception:
        pass
    cur.execute("CREATE INDEX IF NOT EXISTS idx_people_company_id ON people(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_people_last_first ON people(last_name, first_name);")

    # Outreach tables
    cur.execute(
        (
            "CREATE TABLE IF NOT EXISTS outreach_templates (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  name TEXT NOT NULL,\n"
            "  channel TEXT NOT NULL,\n"
            "  body_md TEXT NOT NULL,\n"
            "  variables_json TEXT,\n"
            "  is_active INTEGER NOT NULL DEFAULT 1,\n"
            "  created_at TEXT NOT NULL DEFAULT (datetime('now'))\n"
            ")"
        )
    )

    cur.execute(
        (
            "CREATE TABLE IF NOT EXISTS outreach_messages (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  linkedin_profile TEXT NOT NULL,\n"
            "  channel TEXT NOT NULL,\n"
            "  template_id INTEGER,\n"
            "  stage_no INTEGER NOT NULL DEFAULT 1,\n"
            "  rendered_md TEXT NOT NULL,\n"
            "  status TEXT NOT NULL DEFAULT 'scheduled',\n"
            "  scheduled_at TEXT,\n"
            "  sent_at TEXT,\n"
            "  replied_at TEXT,\n"
            "  created_at TEXT NOT NULL DEFAULT (datetime('now')),\n"
            "  FOREIGN KEY(template_id) REFERENCES outreach_templates(id) ON DELETE SET NULL\n"
            ")"
        )
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_outreach_messages_status ON outreach_messages(status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_outreach_messages_scheduled ON outreach_messages(scheduled_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_outreach_messages_channel ON outreach_messages(channel);")

    # View for joined reads
    cur.execute("DROP VIEW IF EXISTS v_people_with_company;")
    cur.execute(
        (
            "CREATE VIEW v_people_with_company AS\n"
            "SELECT\n"
            "  p.id AS person_id,\n"
            "  p.first_name,\n"
            "  p.last_name,\n"
            "  p.linkedin_profile,\n"
            "  p.title_current,\n"
            "  p.email,\n"
            "  p.location_text,\n"
            "  p.connections_linkedin,\n"
            "  p.followers_linkedin,\n"
            "  p.website_info,\n"
            "  p.phone_info,\n"
            "  p.info_raw,\n"
            "  p.insights_text,\n"
            "  p.lookup_date,\n"
            "  p.is_hot,\n"
            "  p.status,\n"
            "  p.notes,\n"
            "  p.last_interaction_date,\n"
            "  c.id AS company_id,\n"
            "  c.name AS company_name,\n"
            "  c.domain AS domain,\n"
            "  c.website AS website,\n"
            "  c.size_employees,\n"
            "  c.legal_form,\n"
            "  c.industries_json,\n"
            "  c.locations_de_json,\n"
            "  c.multinational,\n"
            "  c.last_enriched_at\n"
            "FROM people p LEFT JOIN companies c ON p.company_id = c.id;"
        )
    )

    conn.commit()



