import json
import logging
import os

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

    return {
        "id": f"apollo_{person['id']}",
        "name": person.get("name", "Unknown"),
        "title": person.get("title", ""),
        "company": org.get("name", "Unknown"),
        "company_size": org.get("num_employees") or 0,
        "linkedin_url": person.get("linkedin_url"),
        "email": person.get("email"),
        "location": location,
        "signals": {
            "datastax_signal": datastax_at_current_org,
            "cassandra_usage": False,       # not surfaced by Apollo; add enrichment layer to fill this
            "datastax_employee_history": ds_alumni,
            "recent_cassandra_post": False,  # not surfaced by Apollo
            "tech_stack_mentions": [],
        },
        "activity_days_ago": 30,  # Apollo free tier omits activity timestamps; default to 30
        "bio": person.get("headline") or "",
        "pain_points": [],
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
        leads = json.load(f)
    logger.info("Loaded %d raw leads from %s", len(leads), pool_path)
    return leads


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

    for lead in all_leads:
        db.upsert_lead(lead)

    qualified = [l for l in all_leads if _pre_qualify(l)]
    disqualified = [l for l in all_leads if not _pre_qualify(l)]

    for lead in disqualified:
        db.update_lead_status(lead["id"], "disqualified")
        logger.debug("  Pre-filtered: %-30s  [%s]", lead["name"], lead["title"])

    logger.info(
        "Pre-qualification: %d/%d passed  (%d disqualified by title)",
        len(qualified),
        len(all_leads),
        len(disqualified),
    )
    return qualified, all_leads
