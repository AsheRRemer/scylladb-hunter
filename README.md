# ScyllaDB GTM Hunter

**What it does:** Finds the right people at the right companies, scores them on how likely they are to switch from DataStax or Cassandra, and drafts a personalized outreach message for each one — automatically.

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
| `output/report.html` | A visual pipeline funnel showing every lead, their score, why they were selected, and the exact messages drafted for them. Open in any browser. |
| `output/dry_run_log.json` | A full audit trail of every outreach event: who, when, what was sent, and what score triggered it. |
| `output/leads.db` | A SQLite database with the complete lead and message history for follow-up or CRM import. |

---

## How Leads Are Scored

Every lead gets a score from 0–100 based on four factors:

| Factor | Weight | What it measures |
|---|---|---|
| **Title seniority** | 35% | CTO/VP scores highest; junior engineers score lowest. We want decision-makers. |
| **DataStax signal** | 30% | Do they mention DataStax Enterprise, Cassandra, or related tools in their profile? The stronger the signal, the more relevant the conversation. |
| **Company size** | 20% | Larger companies have larger infrastructure bills — and larger potential deals. |
| **Activity recency** | 15% | Someone who posted about Cassandra 3 days ago is warmer than someone who hasn't been active in 3 months. |

Only leads above the configured threshold (default: **60**) receive outreach messages.

---

## How Messages Are Written

For each selected lead, the AI drafts a personalized message that:

- References their specific role and company context
- Connects ScyllaDB's real technical advantages to their likely pain points (GC pauses, licensing cost, operational overhead)
- Ends with a single, low-friction call to action

The AI knows ScyllaDB's actual differentiators: 10x lower p99 latency, elimination of JVM GC pauses, 80–90% infrastructure cost reduction vs equivalent Cassandra deployments, and full Cassandra API compatibility (no rewrites needed).

Messages are drafted but **not sent** by default (`dry_run: true` in `config.yaml`). A human reviews and sends.

---

## Quick Start

```bash
# 1. Install dependencies (Python 3.11+)
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
  max_leads: 10              # How many leads to process
  min_score_threshold: 60    # Minimum score to receive outreach
  dry_run: true              # true = draft only, false = log as sent
  message_types:
    - linkedin_dm
    - email
```

To run against a real list, replace `data/leads_seed.json` with your own lead data following the same JSON structure.

---

## The Funnel at a Glance

```
Gathered → Scored → Selected → Messaged
   10         10        7          7
```

Each stage is visible in the HTML report with lead-by-lead detail.

---

## What ScyllaDB Gets From This

A senior engineer at a Comcast or T-Mobile who is actively feeling DataStax licensing pain is worth an enormous amount. This tool surfaces them systematically, ensures outreach is relevant and human-feeling, and keeps a full audit trail — so the GTM team spends time on conversations, not research.

---

*Built as a home assignment for ScyllaDB. Powered by the Anthropic Claude API.*
