import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from hunter.db import Database

logger = logging.getLogger(__name__)


def log_outreach(
    lead: dict,
    message_type: str,
    message: str,
    score: float,
    log_path: str,
    db: Database,
    dry_run: bool = True,
) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lead_id": lead["id"],
        "lead_name": lead["name"],
        "title": lead["title"],
        "company": lead["company"],
        "message_type": message_type,
        "score": score,
        "dry_run": dry_run,
        "message": message,
    }

    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    existing: list[dict] = []
    if log_file.exists():
        try:
            existing = json.loads(log_file.read_text())
        except json.JSONDecodeError:
            existing = []

    existing.append(entry)
    log_file.write_text(json.dumps(existing, indent=2))

    db.add_message(lead["id"], message_type, message)

    mode = "[DRY RUN]" if dry_run else "[LIVE]"
    logger.info(
        "%s %s → %s @ %s (score: %.1f)",
        mode,
        message_type,
        lead["name"],
        lead["company"],
        score,
    )
