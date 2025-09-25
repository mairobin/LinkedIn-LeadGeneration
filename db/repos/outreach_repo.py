from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


class OutreachRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_template(self, name: str, channel: str, body_md: str, variables_json: Optional[str] = None) -> int:
        sql = (
            "INSERT INTO outreach_templates (name, channel, body_md, variables_json, is_active) "
            "VALUES (?, ?, ?, ?, 1) RETURNING id;"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (name, channel, body_md, variables_json))
        row = cur.fetchone()
        return int(row[0])

    def schedule_message(
        self,
        linkedin_profile: str,
        channel: str,
        template_id: Optional[int],
        stage_no: int,
        rendered_md: str,
        when_iso: Optional[str],
    ) -> int:
        sql = (
            "INSERT INTO outreach_messages (linkedin_profile, channel, template_id, stage_no, rendered_md, status, scheduled_at) "
            "VALUES (?, ?, ?, ?, ?, 'scheduled', ?) RETURNING id;"
        )
        cur = self.conn.cursor()
        cur.execute(sql, (linkedin_profile, channel, template_id, stage_no, rendered_md, when_iso))
        row = cur.fetchone()
        return int(row[0])

    def mark_sent(self, message_id: int) -> None:
        sql = "UPDATE outreach_messages SET status='sent', sent_at=datetime('now') WHERE id=?;"
        self.conn.execute(sql, (message_id,))
        self.conn.commit()

    def mark_replied(self, message_id: int) -> None:
        sql = "UPDATE outreach_messages SET status='replied', replied_at=datetime('now') WHERE id=?;"
        self.conn.execute(sql, (message_id,))
        self.conn.commit()

    def due_messages(self, now_iso: Optional[str] = None) -> List[Tuple]:
        if now_iso:
            sql = (
                "SELECT id, linkedin_profile, stage_no, channel, rendered_md, scheduled_at "
                "FROM outreach_messages WHERE status='scheduled' AND scheduled_at <= ? ORDER BY scheduled_at ASC;"
            )
            cur = self.conn.cursor()
            cur.execute(sql, (now_iso,))
            return cur.fetchall()
        else:
            sql = (
                "SELECT id, linkedin_profile, stage_no, channel, rendered_md, scheduled_at "
                "FROM outreach_messages WHERE status='scheduled' AND scheduled_at <= datetime('now') ORDER BY scheduled_at ASC;"
            )
            cur = self.conn.cursor()
            cur.execute(sql)
            return cur.fetchall()



