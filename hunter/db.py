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
        self._migrate()

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
                confidence_score REAL,
                decision TEXT,
                score_breakdown TEXT,
                status TEXT DEFAULT 'gathered',
                field_confidence TEXT,
                tenure_months INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                content TEXT NOT NULL,
                step INTEGER,
                scheduled_day INTEGER,
                sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id TEXT NOT NULL,
                message_id INTEGER,
                channel TEXT NOT NULL,
                sentiment TEXT NOT NULL,
                response_text TEXT,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id),
                FOREIGN KEY (message_id) REFERENCES messages(id)
            );
        """)
        self.conn.commit()

    def _migrate(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id TEXT NOT NULL,
                message_id INTEGER,
                channel TEXT NOT NULL,
                sentiment TEXT NOT NULL,
                response_text TEXT,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id),
                FOREIGN KEY (message_id) REFERENCES messages(id)
            );
        """)
        for stmt in [
            "ALTER TABLE messages ADD COLUMN step INTEGER",
            "ALTER TABLE messages ADD COLUMN scheduled_day INTEGER",
            "ALTER TABLE leads ADD COLUMN field_confidence TEXT",
            "ALTER TABLE leads ADD COLUMN tenure_months INTEGER",
            "ALTER TABLE leads ADD COLUMN confidence_score REAL",
            "ALTER TABLE leads ADD COLUMN decision TEXT",
        ]:
            try:
                self.conn.execute(stmt)
            except sqlite3.OperationalError:
                pass  # column already exists
        self.conn.commit()

    def get_messaged_lead_ids(self) -> set:
        rows = self.conn.execute(
            "SELECT id FROM leads WHERE status = 'messaged'"
        ).fetchall()
        return {row["id"] for row in rows}

    def upsert_lead(self, lead: dict):
        self.conn.execute(
            """
            INSERT OR IGNORE INTO leads
              (id, name, title, company, company_size, linkedin_url, email,
               location, signals, activity_days_ago, bio, pain_points,
               field_confidence, tenure_months, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'gathered')
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
                json.dumps(lead.get("field_confidence", {})),
                lead.get("tenure_months"),
            ),
        )
        self.conn.commit()

    def update_lead_enrichment(self, lead: dict):
        """Persist email, pain_points, and field_confidence after enrichment."""
        self.conn.execute(
            """UPDATE leads SET email = ?, pain_points = ?, field_confidence = ?, tenure_months = ?
               WHERE id = ?""",
            (
                lead.get("email"),
                json.dumps(lead.get("pain_points", [])),
                json.dumps(lead.get("field_confidence", {})),
                lead.get("tenure_months"),
                lead["id"],
            ),
        )
        self.conn.commit()

    def update_lead_score(self, lead_id: str, icp_score: float,
                          confidence_score: float, decision: str, breakdown: dict):
        self.conn.execute(
            """UPDATE leads
               SET score = ?, confidence_score = ?, decision = ?,
                   score_breakdown = ?, status = 'scored'
               WHERE id = ?""",
            (icp_score, confidence_score, decision, json.dumps(breakdown), lead_id),
        )
        self.conn.commit()

    def update_lead_status(self, lead_id: str, status: str):
        self.conn.execute("UPDATE leads SET status = ? WHERE id = ?", (status, lead_id))
        self.conn.commit()

    def add_message(self, lead_id: str, message_type: str, content: str,
                    step=None, scheduled_day=None) -> int:
        cursor = self.conn.execute(
            """INSERT INTO messages (lead_id, message_type, content, step, scheduled_day)
               VALUES (?, ?, ?, ?, ?)""",
            (lead_id, message_type, content, step, scheduled_day),
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_response(self, lead_id: str, channel: str, sentiment: str,
                     message_id=None, response_text=None) -> int:
        cursor = self.conn.execute(
            """INSERT INTO responses (lead_id, message_id, channel, sentiment, response_text)
               VALUES (?, ?, ?, ?, ?)""",
            (lead_id, message_id, channel, sentiment, response_text),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_responses_for_lead(self, lead_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM responses WHERE lead_id = ? ORDER BY received_at",
            (lead_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_leads(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM leads ORDER BY COALESCE(score, -1) DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_messages_for_lead(self, lead_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM messages WHERE lead_id = ? ORDER BY step, sent_at",
            (lead_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
