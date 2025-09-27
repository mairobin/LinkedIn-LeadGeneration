from __future__ import annotations

import sqlite3

from db import schema
from db.repos.queries_repo import QueriesRepo


def test_queries_repo_normalizes_and_is_unique(tmp_path):
    db = sqlite3.connect(str(tmp_path / "t.db"))
    try:
        schema.bootstrap(db)
        repo = QueriesRepo(db)
        q1 = repo.find_or_create("linkedin_people_google", "person", " Engineer   Berlin ")
        q2 = repo.find_or_create("linkedin_people_google", "person", "engineer berlin")
        assert q1 == q2
        # Different source should produce a different row
        q3 = repo.find_or_create("another_source", "person", "engineer berlin")
        assert q3 != q1
    finally:
        db.close()


