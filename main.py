import logging

import yaml

from hunter.db import Database
from hunter.enricher import enrich
from hunter.lead_finder import load_leads
from hunter.personalizer import generate_message
from hunter.reporter import generate_report
from hunter.scorer import decide, score_lead
from hunter.trigger import run_sequence

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
    output = cfg["output"]

    db = Database(output["db_file"])

    # ── 1. Gather & pre-qualify ───────────────────────────────────────────────
    logger.info("=== STAGE 1: GATHER ===")
    qualified, all_leads = load_leads(cfg, db)

    # ── 1.5. Enrich ──────────────────────────────────────────────────────────
    logger.info("=== STAGE 1.5: ENRICH ===")
    qualified = enrich(qualified, cfg, pipeline["dry_run"])
    for lead in qualified:
        db.update_lead_enrichment(lead)

    # ── 2. Score ─────────────────────────────────────────────────────────────
    logger.info("=== STAGE 2: SCORE ===")
    icp_threshold = pipeline["min_score_threshold"]
    confidence_threshold = scoring.get("confidence_threshold", 70)
    for lead in qualified:
        icp_score, confidence_score, breakdown = score_lead(lead, scoring)
        decision = decide(icp_score, confidence_score, icp_threshold, confidence_threshold)
        db.update_lead_score(lead["id"], icp_score, confidence_score, decision, breakdown)
        lead["score"] = icp_score
        lead["confidence_score"] = confidence_score
        lead["decision"] = decision
        lead["score_breakdown"] = breakdown
        icp = breakdown["icp"]
        logger.info(
            "  %-32s %-22s → icp:%5.1f  conf:%5.1f  [%s]",
            lead["name"], lead["company"],
            icp_score, confidence_score, decision,
        )
        logger.info(
            "    title:%.0f size:%.0f signal:%.0f recency:%.0f",
            icp["title"], icp["company_size"], icp["datastax_signal"], icp["activity_recency"],
        )

    # ── 3+4. Sequence engine ──────────────────────────────────────────────────
    dry_run = pipeline["dry_run"]
    logger.info("=== STAGE 3+4: SEQUENCE ENGINE [%s] ===", "DRY RUN" if dry_run else "LIVE")
    stats = run_sequence(qualified, cfg, db, generate_message, dry_run)
    logger.info(
        "  %d evaluated → %d qualified → %d steps executed, %d skipped",
        stats["evaluated"],
        stats["qualified"],
        stats["steps_executed"],
        stats["steps_skipped"],
    )

    # ── 5. Report ─────────────────────────────────────────────────────────────
    logger.info("=== STAGE 5: REPORT ===")
    generate_report(db, output["report_file"])

    db.close()

    logger.info(
        "Pipeline complete. %d gathered → %d scored → %d selected → %d messaged",
        len(all_leads),
        len(qualified),
        stats["qualified"],
        stats["qualified"],
    )
    logger.info("Outputs:")
    logger.info("  Report  → %s", output["report_file"])
    logger.info("  Log     → %s", output["log_file"])
    logger.info("  DB      → %s", output["db_file"])


if __name__ == "__main__":
    main()
