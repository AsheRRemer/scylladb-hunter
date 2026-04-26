import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from hunter.db import Database
# from hunter.hubspot import log_contact, log_outbound_email  # uncomment when HUBSPOT_API_KEY is configured

logger = logging.getLogger(__name__)

SEQUENCE = [
    {"step": 1, "day": 0,  "message_type": "linkedin_connection"},
    {"step": 2, "day": 3,  "message_type": "email"},
    {"step": 3, "day": 7,  "message_type": "email_followup"},
]


def _simulate(lead_id: str, event: str, rate: float) -> bool:
    """Deterministic outcome — same lead always produces the same simulated result."""
    digest = int(hashlib.md5(f"{lead_id}:{event}".encode()).hexdigest(), 16)
    return (digest % 1000) < int(rate * 1000)


def _append_log(entry: dict, log_path: str):
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def _log_entry(lead: dict, step_def: dict, score: float, dry_run: bool,
               message=None, skip_reason=None) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lead_id": lead["id"],
        "lead_name": lead["name"],
        "company": lead["company"],
        "score": score,
        "step": step_def["step"],
        "message_type": step_def["message_type"],
        "scheduled_day": step_def["day"],
        "dry_run": dry_run,
        "message_content": message,
        "skip_reason": skip_reason,
    }


def run_sequence(
    scored_leads: list[dict],
    cfg: dict,
    db: Database,
    generate_message_fn: Callable,
    dry_run: bool = True,
) -> dict:
    """
    Evaluate all scored leads, run a 3-step outbound sequence for qualifying ones,
    and log every action — including skipped steps — to dry_run_log.json.

    Step 1  Day 0  linkedin_connection   — always fires for qualifying leads
    Step 2  Day 3  email                 — skipped if LinkedIn accepted
    Step 3  Day 7  email_followup        — skipped if LinkedIn accepted or email replied

    Outcomes are simulated deterministically from lead_id so runs are reproducible.
    """
    threshold = cfg["pipeline"]["min_score_threshold"]
    seq_cfg = cfg.get("sequence", {})
    linkedin_accept_rate = seq_cfg.get("linkedin_accept_rate", 0.35)
    email_reply_rate = seq_cfg.get("email_reply_rate", 0.20)
    anthropic_cfg = cfg["anthropic"]
    log_path = cfg["output"]["log_file"]
    mode = "[DRY RUN]" if dry_run else "[LIVE]"

    stats = {"evaluated": 0, "qualified": 0, "steps_executed": 0, "steps_skipped": 0}

    for lead in scored_leads:
        stats["evaluated"] += 1
        score = lead.get("score", 0)
        decision = lead.get("decision", "skip")

        if decision == "skip":
            for step_def in SEQUENCE:
                _append_log(
                    _log_entry(lead, step_def, score, dry_run,
                               skip_reason=f"ICP score {score:.1f} below threshold {threshold}"),
                    log_path,
                )
                stats["steps_skipped"] += 1
            logger.info("  [SKIP] %-28s icp:%.1f < threshold %.0f", lead["name"], score, threshold)
            continue

        stats["qualified"] += 1
        db.update_lead_status(lead["id"], "selected")
        # Uncomment to create/update this lead as a contact in HubSpot CRM (requires HUBSPOT_API_KEY):
        # log_contact(cfg, lead)

        enrich_first = decision == "enrich_first"
        already_sent = db.get_sent_step_numbers(lead["id"])
        linkedin_accepted = _simulate(lead["id"], "linkedin_accepted", linkedin_accept_rate)
        email_replied = _simulate(lead["id"], "email_replied", email_reply_rate)

        conf_score = lead.get("confidence_score", 0)
        logger.info("  [SEQUENCE] %s @ %s (icp:%.1f conf:%.1f decision:%s)",
                    lead["name"], lead["company"], score, conf_score, decision)
        if enrich_first:
            logger.info("    └─ enrich_first: low confidence — LinkedIn connection only, no cold email")
        elif linkedin_accepted:
            logger.info("    └─ simulated: LinkedIn accepted → steps 2+3 will be skipped")
        elif email_replied:
            logger.info("    └─ simulated: email replied on Day 3 → step 3 will be skipped")

        for step_def in SEQUENCE:
            step = step_def["step"]
            day = step_def["day"]
            message_type = step_def["message_type"]

            skip_reason = None
            if step in already_sent:
                skip_reason = f"Step {step} already sent in a previous run"
            elif enrich_first and step > 1:
                skip_reason = "Flagged enrich_first — awaiting enrichment before email outreach"
            elif step == 2 and linkedin_accepted:
                skip_reason = "LinkedIn connection accepted (simulated Day 1) — email not needed"
            elif step == 3 and linkedin_accepted:
                skip_reason = "LinkedIn connection accepted (simulated Day 1) — sequence complete"
            elif step == 3 and email_replied:
                skip_reason = "Reply received on Day 3 email (simulated) — follow-up not needed"

            if skip_reason:
                _append_log(
                    _log_entry(lead, step_def, score, dry_run, skip_reason=skip_reason),
                    log_path,
                )
                stats["steps_skipped"] += 1
                logger.info("    [SKIP] Step %d Day %d %s — %s", step, day, message_type, skip_reason)
                continue

            logger.info("    %s Step %d Day %d %s", mode, step, day, message_type)
            try:
                message = generate_message_fn(
                    lead, message_type, anthropic_cfg["model"], anthropic_cfg["max_tokens"]
                )
            except Exception as exc:
                logger.error("    Failed to generate: %s", exc)
                _append_log(
                    _log_entry(lead, step_def, score, dry_run,
                               skip_reason=f"Generation error: {exc}"),
                    log_path,
                )
                stats["steps_skipped"] += 1
                continue

            _append_log(
                _log_entry(lead, step_def, score, dry_run, message=message),
                log_path,
            )
            db.add_message(lead["id"], message_type, message, step=step, scheduled_day=day)
            # Uncomment to log sent emails to HubSpot CRM (requires HUBSPOT_API_KEY):
            # if message_type in ("email", "email_followup"):
            #     log_outbound_email(cfg, lead, subject="", body=message)
            stats["steps_executed"] += 1

        final_status = "linkedin_sent" if enrich_first else "messaged"
        db.update_lead_status(lead["id"], final_status)

    return stats
