TITLE_SENIORITY_MAP = {
    "cto": 100,
    "chief technology officer": 100,
    "cio": 95,
    "svp": 90,
    "evp": 90,
    "vp": 85,
    "vice president": 85,
    "head of": 80,
    "director": 75,
    "architect": 70,
    "principal": 65,
    "staff": 60,
    "lead": 55,
    "manager": 55,
    "senior": 50,
    "engineer": 30,
    "developer": 30,
}

_EMAIL_CONF_SCORES  = {"high": 100, "medium": 60,  "low": 30,  "none": 0}
_BIO_CONF_SCORES    = {"high": 100, "inferred": 50, "none": 0}
_TENURE_CONF_SCORES = {"known": 100, "partial": 50, "unknown": 0}


def _score_title(title: str) -> float:
    title_lower = title.lower()
    best = 20
    for keyword, score in TITLE_SENIORITY_MAP.items():
        if keyword in title_lower:
            best = max(best, score)
    return float(best)


def _score_company_size(size: int) -> float:
    if size >= 50000: return 100
    if size >= 10000: return 90
    if size >= 5000:  return 80
    if size >= 1000:  return 65
    if size >= 500:   return 50
    if size >= 100:   return 35
    return 20


def _score_datastax_signal(signals: dict) -> float:
    score = 0
    if signals.get("datastax_signal"):          score += 45
    if signals.get("cassandra_usage"):           score += 30
    if signals.get("datastax_employee_history"): score += 15
    if signals.get("recent_cassandra_post"):     score += 10
    return min(float(score), 100)


def _score_activity_recency(days_ago: int) -> float:
    if days_ago <= 2:  return 100
    if days_ago <= 7:  return 85
    if days_ago <= 14: return 70
    if days_ago <= 30: return 50
    if days_ago <= 60: return 30
    if days_ago <= 90: return 15
    return 5


def _score_confidence(field_confidence: dict) -> tuple:
    """
    Returns (confidence_score, confidence_breakdown).
    Each field is scored independently so a missing bio never tanks a high-signal lead.
    """
    email_score  = _EMAIL_CONF_SCORES.get(field_confidence.get("email", "none"), 0)
    bio_score    = _BIO_CONF_SCORES.get(field_confidence.get("bio", "none"), 0)
    tenure_score = _TENURE_CONF_SCORES.get(field_confidence.get("tenure", "unknown"), 0)
    total = round((email_score + bio_score + tenure_score) / 3, 1)
    return total, {"email": email_score, "bio": bio_score, "tenure": tenure_score}


def decide(icp_score: float, confidence_score: float,
           icp_threshold: float, confidence_threshold: float) -> str:
    """
    HIGH FIT + HIGH CONFIDENCE → selected (full 3-step sequence)
    HIGH FIT + LOW CONFIDENCE  → enrich_first (linkedin_connection only, no cold email yet)
    LOW FIT                    → skip (never rejected solely for missing data)
    """
    if icp_score < icp_threshold:
        return "skip"
    if confidence_score < confidence_threshold:
        return "enrich_first"
    return "selected"


def score_lead(lead: dict, weights: dict) -> tuple:
    title_score    = _score_title(lead.get("title", ""))
    size_score     = _score_company_size(lead.get("company_size", 0))
    datastax_score = _score_datastax_signal(lead.get("signals", {}))
    recency_score  = _score_activity_recency(lead.get("activity_days_ago", 999))

    icp_score = round(
        title_score    * weights.get("title_seniority_weight", 0.35)
        + size_score     * weights.get("company_size_weight", 0.20)
        + datastax_score * weights.get("datastax_signal_weight", 0.40)
        + recency_score  * weights.get("activity_recency_weight", 0.05),
        1,
    )

    confidence_score, conf_breakdown = _score_confidence(lead.get("field_confidence", {}))

    breakdown = {
        "icp": {
            "title":           round(title_score, 1),
            "company_size":    round(size_score, 1),
            "datastax_signal": round(datastax_score, 1),
            "activity_recency": round(recency_score, 1),
            "total":           icp_score,
        },
        "confidence": {
            **conf_breakdown,
            "total": confidence_score,
        },
    }

    return icp_score, confidence_score, breakdown
