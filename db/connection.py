from __future__ import annotations

import sqlite3
from typing import Optional


def get_connection(db_path: str, timeout: Optional[float] = 30.0) -> sqlite3.Connection:
    """Open a SQLite connection with sane pragmas for concurrent local use.

    - WAL journal for fewer writer blocks
    - NORMAL synchronous for performance
    - foreign_keys ON to enforce integrity
    """
    conn = sqlite3.connect(db_path, timeout=timeout or 30.0)
    # Pragmas
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn




