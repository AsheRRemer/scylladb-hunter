import logging

import anthropic

logger = logging.getLogger(__name__)

_DIFFERENTIATORS = """
ScyllaDB vs DataStax/Apache Cassandra — key facts for your pitch:

PERFORMANCE
- ScyllaDB delivers 10x lower p99 latency vs Apache Cassandra (single-digit ms vs 50–200ms)
- Shard-per-core C++ architecture eliminates JVM GC pauses entirely
- 1M+ ops/sec per node vs ~100K for Cassandra on equivalent hardware

COST
- Replace 10 Cassandra nodes with 1 ScyllaDB node — 80–90% infrastructure reduction
- DataStax Enterprise adds per-node licensing on top of infrastructure; ScyllaDB Enterprise
  is consumption-based with no per-node tax
- Native compaction uses 50% less storage amplification than Cassandra's strategies

COMPATIBILITY
- Drop-in replacement for Cassandra — same CQL, same drivers, same data model
- No application rewrites required; works with existing Cassandra clients
- ScyllaDB Cloud available on AWS, GCP, Azure

OPERATIONS
- ScyllaDB auto-tunes internally using its reactive framework; no JVM heap tuning
- Significantly lower compaction overhead — fewer compaction storms under write load
- Discord, Comcast, Grab, and others migrated with zero downtime
"""


def generate_message(lead: dict, message_type: str, model: str, max_tokens: int) -> str:
    client = anthropic.Anthropic()

    signals = lead.get("signals", {})
    tech_stack = signals.get("tech_stack_mentions", [])
    pain_points = lead.get("pain_points", [])

    if message_type == "linkedin_connection":
        format_instructions = (
            "Write a LinkedIn connection request note. "
            "Hard limit: 250 characters (LinkedIn enforces this). "
            "Do NOT pitch the product — just establish why you're reaching out. "
            "Reference their specific role or company. Human and direct."
        )
    elif message_type == "linkedin_dm":
        format_instructions = (
            "Write a LinkedIn DM. Max 150 words. No subject line. "
            "Peer-to-peer tone, not marketing copy. "
            "End with one specific, low-friction CTA (e.g., '15-min call', 'benchmark together')."
        )
    elif message_type == "email_followup":
        format_instructions = (
            "Write a follow-up email to a previous outreach that received no reply. "
            "Start with 'Subject: <subject line>' then a blank line then the body. "
            "Body max 100 words. Acknowledge this is a follow-up without being apologetic. "
            "Lead with a new data point or different angle — not a copy of the first email. "
            "Single low-friction CTA. Do NOT mention ScyllaDB in the subject line."
        )
    else:  # email
        format_instructions = (
            "Write an email. Start with 'Subject: <subject line>' then a blank line then the body. "
            "Body max 200 words. Professional but human tone. "
            "End with one specific, low-friction CTA. Do NOT mention ScyllaDB in the subject line."
        )

    prompt = f"""You are a ScyllaDB GTM engineer reaching out to a qualified prospect.

Lead:
  Name: {lead["name"]}
  Title: {lead["title"]}
  Company: {lead["company"]} (~{lead["company_size"]:,} employees)
  Known tech stack: {", ".join(tech_stack) if tech_stack else "Cassandra"}
  Pain points: {" | ".join(pain_points) if pain_points else "latency, cost, ops overhead"}
  Bio: {lead.get("bio", "")}

{_DIFFERENTIATORS}

Task: {format_instructions}

Rules:
- Reference 1–2 differentiators most relevant to THIS person's specific pain points
- Mention their company or role context at least once to show it's not a blast
- Do NOT use em dashes (—) anywhere in the message
- Output ONLY the message, no preamble or explanation"""

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text
