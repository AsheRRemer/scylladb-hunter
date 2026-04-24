import json
import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS leads (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                title TEXT,
                company TEXT,
                company_size INTEGER,
                linkedin_url TEXT,
                email TEXT,
                location TEXT,
                signals TEXT,
                activity_days_ago INTEGER,
                bio TEXT,
                pain_points TEXT,
                score REAL,
                score_breakdown TEXT,
                status TEXT DEFAULT 'gathered',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                content TEXT NOT NULL,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );
        """)
        self.conn.commit()

    def upsert_lead(self, lead: dict):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO leads
              (id, name, title, company, company_size, linkedin_url, email,
               location, signals, activity_days_ago, bio, pain_points, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gathered')
            """,
            (
                lead["id"],
                lead["name"],
                lead["title"],
                lead["company"],
                lead["company_size"],
                lead.get("linkedin_url"),
                lead.get("email"),
                lead.get("location"),
                json.dumps(lead.get("signals", {})),
                lead.get("activity_days_ago", 999),
                lead.get("bio", ""),
                json.dumps(lead.get("pain_points", [])),
            ),
        )
        self.conn.commit()

    def update_lead_score(self, lead_id: str, score: float, breakdown: dict):
        self.conn.execute(
            "UPDATE leads SET score = ?, score_breakdown = ?, status = 'scored' WHERE id = ?",
            (score, json.dumps(breakdown), lead_id),
        )
        self.conn.commit()

    def update_lead_status(self, lead_id: str, status: str):
        self.conn.execute("UPDATE leads SET status = ? WHERE id = ?", (status, lead_id))
        self.conn.commit()

    def add_message(self, lead_id: str, message_type: str, content: str) -> int:
        cursor = self.conn.execute(
            "INSERT INTO messages (lead_id, message_type, content) VALUES (?, ?, ?)",
            (lead_id, message_type, content),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_all_leads(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM leads ORDER BY COALESCE(score, -1) DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_messages_for_lead(self, lead_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM messages WHERE lead_id = ? ORDER BY sent_at",
            (lead_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
