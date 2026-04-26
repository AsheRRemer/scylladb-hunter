import json
import logging
import os
import re
from datetime import datetime

import requests

from hunter.db import Database

logger = logging.getLogger(__name__)

APOLLO_SEARCH_URL = "https://api.apollo.io/v1/mixed_people/search"

# Title words that immediately disqualify a lead before scoring.
# These roles will never convert regardless of company or seniority.
_DISQUALIFYING_WORDS = frozenset({
    "intern", "internship",
    "sales",
    "marketing",
    "recruiter", "recruiting",
    "hr",
})

_DISQUALIFYING_PHRASES = frozenset({
    "account executive",
    "account manager",
    "business development",
    "customer success",
    "customer support",
    "human resources",
    "talent acquisition",
    "people ops",
})


def _pre_qualify(lead: dict) -> bool:
    title_lower = lead.get("title", "").lower()
    title_words = set(title_lower.split())
    return not (
        title_words & _DISQUALIFYING_WORDS
        or any(phrase in title_lower for phrase in _DISQUALIFYING_PHRASES)
    )


def _extract_domain(website_url: str) -> str:
    if not website_url:
        return ""
    m = re.search(r'(?:https?://)?(?:www\.)?([^/\s]+)', website_url)
    return m.group(1) if m else ""


def _calc_tenure(employment: list):
    """Returns (tenure_months | None, confidence: known/partial/unknown)."""
    current = next((e for e in employment if e.get("current")), None)
    if not current:
        return None, "unknown"
    start_str = current.get("start_date")
    if not start_str:
        return None, "partial"
    try:
        start = datetime.strptime(start_str[:7], "%Y-%m")
        now = datetime.now()
        months = (now.year - start.year) * 12 + (now.month - start.month)
        return max(0, months), "known"
    except ValueError:
        return None, "partial"


def _normalize_apollo_person(person: dict) -> dict:
    """Map a raw Apollo API person record to our internal lead schema."""
    org = person.get("organization") or {}
    city = person.get("city") or ""
    state = person.get("state") or ""
    location = ", ".join(p for p in [city, state] if p)

    org_name_lower = (org.get("name") or "").lower()
    datastax_at_current_org = "datastax" in org_name_lower

    employment = person.get("employment_history") or []
    ds_alumni = any(
        "datastax" in (e.get("organization_name") or "").lower()
        for e in employment
        if not e.get("current")
    )
    tenure_months, tenure_conf = _calc_tenure(employment)

    techs = person.get("technologies") or []
    techs_lower = [t.lower() for t in techs]
    cassandra_from_tech = any("cassandra" in t for t in techs_lower)
    datastax_from_tech = any("datastax" in t for t in techs_lower)
    _TECH_KEYWORDS = {"cassandra", "datastax", "kafka", "redis", "mongodb", "elasticsearch", "scylla", "spark"}
    tech_stack_mentions = [t for t in techs if any(kw in t.lower() for kw in _TECH_KEYWORDS)]

    email_val = person.get("email")
    email_status = (person.get("email_status") or "").lower()
    if email_val and email_status == "verified":
        email_conf = "high"
    elif email_val and email_status in ("guessed", "likely"):
        email_conf = "low"
    elif email_val:
        email_conf = "medium"
    else:
        email_conf = "none"

    bio_conf = "medium" if person.get("headline") else "none"

    person_id = person.get("id", "unknown")
    return {
        "id": f"apollo_{person_id}",
        "first_name": person.get("first_name", ""),
        "last_name": person.get("last_name", ""),
        "name": person.get("name", "Unknown"),
        "title": person.get("title", ""),
        "company": org.get("name", "Unknown"),
        "company_size": org.get("num_employees") or 0,
        "domain": _extract_domain(org.get("website_url", "")),
        "seniority": person.get("seniority", ""),
        "linkedin_url": person.get("linkedin_url"),
        "email": email_val,
        "location": location,
        "signals": {
            "datastax_signal": datastax_at_current_org or datastax_from_tech,
            "cassandra_usage": cassandra_from_tech,
            "datastax_employee_history": ds_alumni,
            "recent_cassandra_post": False,
            "tech_stack_mentions": tech_stack_mentions,
        },
        "activity_days_ago": 30,
        "bio": person.get("headline") or "",
        "pain_points": [],
        "tenure_months": tenure_months,
        "field_confidence": {
            "email": email_conf,
            "bio": bio_conf,
            "tenure": tenure_conf,
        },
    }


def _fetch_from_apollo(cfg: dict) -> list[dict]:
    apollo_cfg = cfg.get("apollo", {})
    api_key = apollo_cfg.get("api_key") or os.environ.get("APOLLO_API_KEY", "")
    if not api_key:
        raise ValueError(
            "Apollo API key not configured. "
            "Set apollo.api_key in config.yaml or export APOLLO_API_KEY=..."
        )

    max_leads = cfg["pipeline"]["max_leads"]
    payload = {
        "api_key": api_key,
        "organization_names": apollo_cfg.get("organization_names", ["DataStax"]),
        "person_titles": apollo_cfg.get("titles", [
            "engineer", "architect", "infrastructure",
            "database", "backend", "CTO", "VP Engineering",
        ]),
        "person_seniorities": apollo_cfg.get("seniorities", [
            "senior", "manager", "director", "vp", "c_suite", "entry",
        ]),
        "per_page": min(max_leads * 3, 100),  # over-fetch so pre-filter has material to work with
        "page": 1,
    }

    logger.info("Calling Apollo People Search → %s", payload["organization_names"])
    resp = requests.post(APOLLO_SEARCH_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    people = data.get("people") or []
    total = data.get("pagination", {}).get("total_entries", "?")
    logger.info("Apollo returned %d people (total available: %s)", len(people), total)

    return [_normalize_apollo_person(p) for p in people]


def _load_from_pool(pool_path: str) -> list[dict]:
    with open(pool_path) as f:
        raw = json.load(f)
    logger.info("Loaded %d raw leads from %s", len(raw), pool_path)
    return [_normalize_apollo_person(p) for p in raw]


def load_leads(cfg: dict, db: Database) -> tuple[list[dict], list[dict]]:
    """
    Load raw leads (live or dry-run), persist all to DB, then pre-qualify.

    Returns:
        (qualified, all_leads)
        qualified  — passed the title pre-filter; these go to the scorer
        all_leads  — every lead loaded, including disqualified ones
    """
    pipeline = cfg["pipeline"]
    dry_run = pipeline["dry_run"]
    max_leads = pipeline["max_leads"]

    if dry_run:
        pool_path = cfg.get("data", {}).get("raw_pool", "data/leads_raw_pool.json")
        raw = _load_from_pool(pool_path)
    else:
        raw = _fetch_from_apollo(cfg)

    all_leads = raw[:max_leads]

    messaged_ids = db.get_messaged_lead_ids()
    new_leads = [l for l in all_leads if l["id"] not in messaged_ids]
    already_messaged = len(all_leads) - len(new_leads)
    if already_messaged:
        logger.info("Dedup: skipped %d lead(s) already messaged in a previous run", already_messaged)

    for lead in new_leads:
        db.upsert_lead(lead)

    qualified = [l for l in new_leads if _pre_qualify(l)]
    disqualified = [l for l in new_leads if not _pre_qualify(l)]

    for lead in disqualified:
        db.update_lead_status(lead["id"], "disqualified")
        logger.debug("  Pre-filtered: %-30s  [%s]", lead["name"], lead["title"])

    logger.info(
        "Pre-qualification: %d/%d passed  (%d disqualified by title)",
        len(qualified),
        len(new_leads),
        len(disqualified),
    )
    return qualified, all_leads
