import logging

import yaml

from hunter.db import Database
from hunter.lead_finder import load_leads
from hunter.personalizer import generate_message
from hunter.reporter import generate_report
from hunter.scorer import score_lead
from hunter.trigger import log_outreach

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)

    pipeline = cfg["pipeline"]
    scoring = cfg["scoring"]
    anthropic_cfg = cfg["anthropic"]
    output = cfg["output"]

    db = Database(output["db_file"])

    # ── 1. Gather & pre-qualify ───────────────────────────────────────────────
    logger.info("=== STAGE 1: GATHER ===")
    qualified, all_leads = load_leads(cfg, db)

    # ── 2. Score ─────────────────────────────────────────────────────────────
    logger.info("=== STAGE 2: SCORE ===")
    for lead in qualified:
        score, breakdown = score_lead(lead, scoring)
        db.update_lead_score(lead["id"], score, breakdown)
        lead["score"] = score
        lead["score_breakdown"] = breakdown
        logger.info(
            "  %-32s %-22s → %5.1f  (title:%.0f size:%.0f signal:%.0f recency:%.0f)",
            lead["name"],
            lead["company"],
            score,
            breakdown["title"],
            breakdown["company_size"],
            breakdown["datastax_signal"],
            breakdown["activity_recency"],
        )

    # ── 3. Select ─────────────────────────────────────────────────────────────
    threshold = pipeline["min_score_threshold"]
    selected = [l for l in qualified if l["score"] >= threshold]
    pre_filtered = len(all_leads) - len(qualified)
    below_threshold = len(qualified) - len(selected)
    logger.info(
        "=== STAGE 3: SELECT === %d selected  (%d pre-filtered, %d below threshold %.0f)",
        len(selected),
        pre_filtered,
        below_threshold,
        threshold,
    )
    for lead in selected:
        db.update_lead_status(lead["id"], "selected")

    # ── 4. Personalize & Trigger ──────────────────────────────────────────────
    dry_run = pipeline["dry_run"]
    mode_label = "DRY RUN" if dry_run else "LIVE"
    logger.info("=== STAGE 4: PERSONALIZE & TRIGGER [%s] ===", mode_label)

    for lead in selected:
        for message_type in pipeline["message_types"]:
            logger.info("  Generating %s for %s…", message_type, lead["name"])
            try:
                message = generate_message(
                    lead,
                    message_type,
                    anthropic_cfg["model"],
                    anthropic_cfg["max_tokens"],
                )
            except Exception as exc:
                logger.error("  Failed to generate message: %s", exc)
                continue

            log_outreach(
                lead,
                message_type,
                message,
                lead["score"],
                output["log_file"],
                db,
                dry_run,
            )

        db.update_lead_status(lead["id"], "messaged")

    # ── 5. Report ─────────────────────────────────────────────────────────────
    logger.info("=== STAGE 5: REPORT ===")
    generate_report(db, output["report_file"])

    db.close()

    logger.info(
        "Pipeline complete. %d gathered → %d scored → %d selected → %d messaged",
        len(all_leads),
        len(qualified),
        len(selected),
        len(selected),
    )
    logger.info("Outputs:")
    logger.info("  Report  → %s", output["report_file"])
    logger.info("  Log     → %s", output["log_file"])
    logger.info("  DB      → %s", output["db_file"])


if __name__ == "__main__":
    main()
