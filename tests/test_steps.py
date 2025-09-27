from __future__ import annotations

import sqlite3

from db import schema
from pipelines.runner import Pipeline, RunContext
from pipelines.steps.validate_people import ValidatePeople
from pipelines.steps.persist_people import PersistPeople


def test_validate_people_skips_strict_for_mapped():
    step = ValidatePeople()
    ctx = RunContext()
    ctx.people = [{
        'Contact_Name': 'Alice Example',
        'LinkedIn_Profile': 'https://linkedin.com/in/alice',
        'Position': 'Engineer',
        'Company': 'Acme'
    }]
    out = step.run(ctx)
    assert out.people and isinstance(out.people, list)


def test_persist_people_inserts_and_links(tmp_path):
    db_path = tmp_path / 't.db'
    conn = sqlite3.connect(str(db_path))
    try:
        schema.bootstrap(conn)
        ppl = [{
            'Contact_Name': 'Dr. Alice Example',
            'LinkedIn_Profile': 'https://linkedin.com/in/alice',
            'Position': 'Engineer',
            'source_name': 'linkedin_people_google',
            'source_query': 'Engineer Berlin',
            'Company': 'Acme',
            'Company_Domain': 'acme.com'
        }]
        pipeline = Pipeline([ValidatePeople(), PersistPeople(conn)])
        ctx = RunContext()
        ctx.people = ppl
        out = pipeline.run(ctx)
        assert out.meta.get('processed_people') == 1
        cur = conn.cursor()
        cur.execute("SELECT first_name, last_name, title_current, search_query_id FROM people")
        row = cur.fetchone()
        assert row is not None
        first_name, last_name, title_current, search_query_id = row
        # Degree prefix stripped from name, appended to title in parentheses
        assert first_name == 'Alice' and last_name == 'Example'
        assert '(Dr' in title_current or '(Dr.' in title_current
        assert isinstance(search_query_id, int) and search_query_id > 0
        cur.execute("SELECT COUNT(*) FROM companies WHERE domain = ?", ('acme.com',))
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


