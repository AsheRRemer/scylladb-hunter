import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

HUNTER_URL = "https://api.hunter.io/v2/email-finder"

_PAIN_POINT_MAP = {
    "cto": [
        "balancing infrastructure cost vs. performance at scale",
        "evaluating database modernization timelines",
        "pressure to reduce cloud spend without sacrificing availability",
    ],
    "vp": [
        "infrastructure spend trending above budget forecast",
        "on-call fatigue from database incidents",
        "engineering velocity blocked by data platform reliability",
    ],
    "director": [
        "database incidents impacting SLA commitments",
        "infrastructure spend trending above budget forecast",
    ],
    "architect": [
        "designing for sub-10ms p99 latency under write-heavy workloads",
        "managing compaction storms during peak traffic",
        "evaluating Cassandra replacement paths",
    ],
    "infrastructure": [
        "JVM GC pauses causing latency spikes in production",
        "database operational overhead consuming engineering cycles",
        "node count scaling costs outpacing workload growth",
    ],
    "senior": [
        "Cassandra compaction storms degrading write throughput",
        "heap tuning and GC configuration complexity",
    ],
    "default": [
        "high p99 latency under concurrent write load",
        "operational complexity of managing Cassandra clusters",
    ],
}


def _load_hunter_pool(pool_path: str) -> dict:
    """Load the Hunter email pool and index it by lead_id for O(1) lookup."""
    try:
        with open(pool_path) as f:
            records = json.load(f)
        return {r["lead_id"]: r for r in records}
    except FileNotFoundError:
        logger.warning("Hunter pool not found at %s — no dry-run emails will be enriched", pool_path)
        return {}


def _lookup_hunter_pool(lead_id: str, pool: dict):
    """Return (email, confidence) from the dry-run pool, or (None, 'none') if not found."""
    record = pool.get(lead_id)
    if not record:
        return None, "none"
    score = record.get("score", 0)
    email = record["email"]
    if score >= 80:
        return email, "high"
    if score >= 50:
        return email, "medium"
    return email, "low"


def _lookup_hunter(api_key: str, first_name: str, last_name: str, domain: str):
    if not api_key or not domain:
        return None, "none"
    try:
        resp = requests.get(
            HUNTER_URL,
            params={"domain": domain, "first_name": first_name, "last_name": last_name, "api_key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data", {})
        email = data.get("email")
        score = data.get("score", 0)
        if not email:
            return None, "none"
        if score >= 80:
            return email, "high"
        if score >= 50:
            return email, "medium"
        return email, "low"
    except Exception as e:
        logger.warning("Hunter.io lookup failed for %s@%s: %s", first_name, domain, e)
        return None, "none"


def _infer_pain_points(title: str, seniority: str) -> list[str]:
    title_lower = title.lower()
    seniority_lower = (seniority or "").lower()
    for key in ("cto", "vp", "director", "architect", "infrastructure", "senior"):
        if key in title_lower or key in seniority_lower:
            return _PAIN_POINT_MAP[key]
    return _PAIN_POINT_MAP["default"]


def enrich(qualified_leads: list[dict], cfg: dict, dry_run: bool) -> list[dict]:
    hunter_key = (cfg.get("hunter", {}).get("api_key") or os.environ.get("HUNTER_API_KEY", ""))
    hunter_pool = {}
    if dry_run:
        pool_path = cfg.get("data", {}).get("hunter_pool", "data/hunter_email_pool.json")
        hunter_pool = _load_hunter_pool(pool_path)
        logger.info("Hunter pool loaded: %d email(s) available", len(hunter_pool))

    for lead in qualified_leads:
        confidence = lead.setdefault("field_confidence", {
            "email": "none",
            "bio": "none",
            "tenure": "unknown",
        })

        # 1. Email enrichment — attempt Hunter.io if email is null and domain is known
        if not lead.get("email"):
            domain = lead.get("domain", "")
            first_name = lead.get("first_name", "")
            last_name = lead.get("last_name", "")
            if domain and first_name:
                if dry_run:
                    email, conf = _lookup_hunter_pool(lead["id"], hunter_pool)
                else:
                    email, conf = _lookup_hunter(hunter_key, first_name, last_name, domain)
                if email:
                    lead["email"] = email
                    confidence["email"] = conf
                    logger.info("  Email enriched: %-22s → %s (%s)", lead["name"], email, conf)
                else:
                    confidence["email"] = "none"
                    logger.debug("  Email not found: %s", lead["name"])
            else:
                confidence["email"] = "none"

        # 2. Pain point inference — infer from title + seniority when headline is missing
        if not lead.get("bio"):
            lead["pain_points"] = _infer_pain_points(
                lead.get("title", ""), lead.get("seniority", "")
            )
            confidence["bio"] = "inferred"
            logger.debug("  Pain points inferred for %s", lead["name"])

        # 3. Tenure — already set by lead_finder; log unknowns so the scorer can apply the penalty
        if confidence.get("tenure") == "unknown":
            logger.debug("  Tenure unknown for %s — -5 score penalty will apply", lead["name"])

    return qualified_leads
