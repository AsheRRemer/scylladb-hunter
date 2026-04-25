# ScyllaDB GTM Hunter

**What it does:** Finds the right people at the right companies, scores them on how likely they are to switch from DataStax or Cassandra, enriches missing data, and drafts a personalized multi-step outreach sequence for each one — automatically.

---

## The Business Problem

DataStax customers are ScyllaDB's highest-intent prospects. They already understand Cassandra-compatible databases, they're already paying for one, and many of them are frustrated with JVM GC pauses, rising per-node licensing costs, and operational overhead.

The problem is identifying *which* engineers and decision-makers at *which* companies are actively feeling that pain — and reaching them before a competitor does.

This pipeline does that work automatically.

---

## What It Produces

Running this tool takes about 2 minutes and delivers three outputs:

| Output | What it is |
|---|---|
| `output/report.html` | A visual pipeline funnel showing every lead, their ICP and confidence scores, the routing decision, and the exact messages drafted for them. Open in any browser. |
| `output/dry_run_log.json` | A full audit trail of every outreach event: who, when, what was sent, what score triggered it, and why any step was skipped. |
| `output/leads.db` | A SQLite database with the complete lead and message history for follow-up or CRM import. |

---

## Pipeline Stages

```
Gather → Enrich → Score → Sequence → Report
```

### Stage 1 — Gather

Loads leads from Apollo.io's People Search API (or `data/leads_raw_pool.json` in dry-run mode). Applies a title pre-filter to disqualify non-technical roles (sales, HR, recruiting, marketing) before any scoring happens.

Deduplication runs at this stage: leads already marked as `messaged` in the database are skipped on subsequent runs to prevent duplicate outreach.

### Stage 1.5 — Enrich

For each pre-qualified lead, the enricher fills gaps in the raw Apollo data:

- **Email** — if Apollo returned no email, attempts a Hunter.io domain lookup. In dry-run mode, looks up the lead in `data/hunter_email_pool.json` instead of hitting the live API. Assigns a confidence level based on Hunter's score: `high` (≥80), `medium` (≥50), or `low`.
- **Bio / Pain points** — if the Apollo headline is missing, infers relevant pain points from title and seniority level rather than leaving the field blank.
- **Tenure** — calculates months in current role from employment history. Marks as `known`, `partial`, or `unknown`.

Every lead carries a `field_confidence` dict after this stage: `{email, bio, tenure}`.

#### Dry-run email pool

`data/hunter_email_pool.json` simulates the subset of leads whose emails Hunter.io would find. Each entry mirrors a Hunter response and is keyed by `lead_id`:

```json
[
  {
    "lead_id": "apollo_pool_002",
    "first_name": "Lin",
    "last_name": "Zhao",
    "domain": "comcast.com",
    "email": "l.zhao@comcast.com",
    "score": 94
  }
]
```

Leads not in the pool come back with `email_confidence: none` — exactly as they would if Hunter returned no result. This gives full control over which leads get enriched in testing without any randomness.

### Stage 2 — Score

Two independent scores are computed for each lead:

#### ICP Score (0–100) — how good a fit is this person

| Factor | Weight | What it measures |
|---|---|---|
| **Title seniority** | 35% | CTO/VP scores highest; junior engineers score lowest |
| **DataStax signal** | 40% | Cassandra usage, DataStax Enterprise, alumni history |
| **Company size** | 20% | Larger infrastructure = larger pain = larger deal |
| **Activity recency** | 5% | Recent Cassandra activity signals active evaluation |

#### Confidence Score (0–100) — how much we trust the data

Averaged from three field confidence ratings: email (0/30/60/100), bio (0/50/100), tenure (0/50/100).

#### Routing Decision

| ICP | Confidence | Decision |
|---|---|---|
| ≥ threshold | ≥ 70 | `selected` — full 3-step sequence |
| ≥ threshold | < 70 | `enrich_first` — LinkedIn connection only, no cold email until data is filled |
| < threshold | any | `skip` — never rejected solely for missing data |

Default thresholds: ICP `60`, confidence `70` (both configurable in `config.yaml`).

### Stage 3+4 — Sequence Engine

Runs a deterministic simulated outbound sequence for each qualifying lead:

| Step | Day | Channel | Condition |
|---|---|---|---|
| 1 | 0 | LinkedIn connection request | Always fires for `selected` and `enrich_first` |
| 2 | 3 | Email | Skipped if LinkedIn accepted, or if `enrich_first` |
| 3 | 7 | Email follow-up | Skipped if LinkedIn accepted, email replied, or `enrich_first` |

Simulation outcomes (LinkedIn accept rate, email reply rate) are deterministic by lead ID so results are reproducible across runs.

### Stage 5 — Report

Generates `output/report.html` with:

- A 4-stage funnel: Gathered → Scored → Selected → Messaged
- Per-lead cards (click to expand) showing ICP score, confidence score, routing decision, field confidence pills, score breakdown by dimension, inferred pain points, and generated messages

---

## How Messages Are Written

For each selected lead the AI (Claude Haiku) drafts a message tailored to the step type:

| Type | Format | Constraint |
|---|---|---|
| `linkedin_connection` | Connection request note | ≤ 250 characters, no product pitch |
| `linkedin_dm` | Direct message | ≤ 150 words, peer-to-peer tone |
| `email` | Subject + body | ≤ 200 word body |
| `email_followup` | Subject + body, new angle | ≤ 100 word body |

All messages reference the lead's specific role and company, connect ScyllaDB's differentiators to their likely pain points, and end with a single low-friction CTA. Em dashes are explicitly prohibited.

The AI is briefed on ScyllaDB's real technical advantages: 10x lower p99 latency, elimination of JVM GC pauses, 80–90% infrastructure cost reduction, and full Cassandra API compatibility.

Messages are drafted but **not sent** by default (`dry_run: true`). A human reviews before anything goes out.

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set your Anthropic API key
export ANTHROPIC_API_KEY=your_key_here

# 3. Run the pipeline
python main.py

# 4. Open the report
open output/report.html
```

---

## Configuration

Everything is controlled from `config.yaml`:

```yaml
pipeline:
  max_leads: 20
  min_score_threshold: 60    # ICP threshold — leads below this are skipped
  dry_run: true              # true = draft only, false = log as sent

scoring:
  title_seniority_weight: 0.35
  company_size_weight: 0.20
  datastax_signal_weight: 0.40
  activity_recency_weight: 0.05
  confidence_threshold: 70   # below this → enrich_first, not full sequence

sequence:
  linkedin_accept_rate: 0.35  # used in dry-run simulation
  email_reply_rate: 0.20

apollo:
  api_key: ""                # or set APOLLO_API_KEY env var

hubspot:
  api_key: ""                # or set HUBSPOT_API_KEY env var
```

---

## HubSpot Integration (ready, not wired)

`hunter/hubspot.py` contains a complete implementation for:

- Upserting the lead as a HubSpot contact at selection time
- Logging sent emails as CRM email engagements associated to the contact

The calls are present in `trigger.py` as commented-out lines. To activate: set `HUBSPOT_API_KEY` and uncomment the two lines in `trigger.py`.

---


## The Funnel at a Glance

```
Gathered → Scored → Selected → Messaged
   20         12        7          7           ?
```

---

## Project Structure

```
hunter/
  lead_finder.py   — Apollo API fetch + normalization + pre-filter + dedup
  enricher.py      — email lookup, pain point inference, tenure calculation
  scorer.py        — ICP score, confidence score, routing decision
  trigger.py       — multi-step sequence engine with deterministic simulation
  personalizer.py  — Claude-powered message generation (4 message types)
  hubspot.py       — HubSpot contact upsert + email engagement logging
  reporter.py      — HTML report generation
  db.py            — SQLite persistence (leads, messages)
data/
  leads_raw_pool.json   — 20-lead Apollo-format pool for dry-run mode
  hunter_email_pool.json — Hunter.io email lookup results for dry-run mode
output/
  report.html
  dry_run_log.json
  leads.db
config.yaml
main.py
```

---

*Built as a home assignment for ScyllaDB. Powered by the Anthropic Claude API.*
