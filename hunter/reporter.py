import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from hunter.db import Database

logger = logging.getLogger(__name__)


def generate_report(db: Database, output_path: str):
    leads = db.get_all_leads()

    for lead in leads:
        for field in ("signals", "pain_points", "score_breakdown"):
            val = lead.get(field)
            if isinstance(val, str) and val:
                try:
                    lead[field] = json.loads(val)
                except json.JSONDecodeError:
                    lead[field] = {}
            elif not val:
                lead[field] = {} if field != "pain_points" else []

        lead["messages"] = db.get_messages_for_lead(lead["id"])

    gathered = len(leads)
    scored = sum(1 for l in leads if l.get("score") is not None)
    selected = sum(1 for l in leads if l.get("status") in ("selected", "messaged"))
    messaged = sum(1 for l in leads if l.get("status") == "messaged")

    html = _render_html(leads, gathered, scored, selected, messaged)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(html)
    logger.info("Report written to %s", output_path)


def _score_color(score) -> str:
    if score is None:
        return "#94a3b8"
    if score >= 75:
        return "#22c55e"
    if score >= 60:
        return "#f59e0b"
    return "#ef4444"


def _status_pill(status: str) -> str:
    colors = {
        "messaged": ("rgba(34,197,94,0.15)", "#4ade80"),
        "selected": ("rgba(251,191,36,0.15)", "#fbbf24"),
        "scored": ("rgba(148,163,184,0.15)", "#94a3b8"),
        "gathered": ("rgba(148,163,184,0.10)", "#64748b"),
        "disqualified": ("rgba(239,68,68,0.12)", "#f87171"),
    }
    bg, fg = colors.get(status, ("rgba(148,163,184,0.1)", "#94a3b8"))
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 10px;'
        f'border-radius:20px;font-size:0.7rem;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.05em">{status}</span>'
    )


def _render_lead_card(lead: dict, idx: int) -> str:
    score = lead.get("score")
    color = _score_color(score)
    score_display = f"{score:.0f}" if score is not None else "—"
    breakdown = lead.get("score_breakdown") or {}
    signals = lead.get("signals") or {}
    pain_points = lead.get("pain_points") or []
    tech_stack = signals.get("tech_stack_mentions", [])
    messages = lead.get("messages", [])
    card_id = f"card_{idx}"

    tech_tags = "".join(
        f'<span class="tag tag-tech">{t}</span>' for t in tech_stack[:4]
    )
    signal_tags = ""
    if signals.get("datastax_signal"):
        signal_tags += '<span class="tag tag-signal">DataStax customer</span>'
    if signals.get("datastax_employee_history"):
        signal_tags += '<span class="tag tag-ds-alumni">DS alumni</span>'
    if signals.get("recent_cassandra_post"):
        signal_tags += '<span class="tag tag-active">recently active</span>'

    breakdown_html = ""
    if breakdown:
        items = [
            ("Title", breakdown.get("title")),
            ("Company Size", breakdown.get("company_size")),
            ("DataStax Signal", breakdown.get("datastax_signal")),
            ("Recency", breakdown.get("activity_recency")),
        ]
        breakdown_html = '<div class="score-breakdown">' + "".join(
            f'<div class="breakdown-item">'
            f'<div class="breakdown-label">{label}</div>'
            f'<div class="breakdown-value">{val:.0f}</div>'
            f"</div>"
            for label, val in items
            if val is not None
        ) + "</div>"

    pain_html = ""
    if pain_points:
        items_html = "".join(f"<li>{p}</li>" for p in pain_points)
        pain_html = f'<div class="pain-section"><h4>Pain Points</h4><ul class="pain-list">{items_html}</ul></div>'

    messages_html = ""
    if messages:
        cards = ""
        for msg in messages:
            msg_type = msg["message_type"].replace("_", " ").title()
            content = msg["content"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            cards += (
                f'<div class="message-card">'
                f'<div class="message-type-label">{msg_type}</div>'
                f'<div class="message-text">{content}</div>'
                f"</div>"
            )
        messages_html = f'<div class="messages-section"><h4>Generated Messages</h4>{cards}</div>'
    else:
        messages_html = '<div class="messages-section"><p class="no-messages">No messages generated (below threshold or not yet processed)</p></div>'

    return f"""
<div class="lead-card">
  <div class="lead-header" onclick="toggleCard('{card_id}')">
    <div class="score-badge" style="background:color-mix(in srgb,{color} 15%,transparent);border:2px solid {color};color:{color}">
      {score_display}
    </div>
    <div class="lead-info">
      <div class="lead-name">{lead["name"]}</div>
      <div class="lead-meta">{lead["title"]} · {lead["company"]} · {lead.get("location","")}</div>
      <div class="lead-tags">{tech_tags}{signal_tags}</div>
    </div>
    {_status_pill(lead.get("status","gathered"))}
    <div class="expand-icon" id="icon_{card_id}">▸</div>
  </div>
  <div class="lead-detail" id="{card_id}">
    <p class="lead-bio">"{lead.get("bio","")}"</p>
    {breakdown_html}
    {pain_html}
    {messages_html}
  </div>
</div>"""


def _render_html(leads, gathered, scored, selected, messaged) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lead_cards = "".join(_render_lead_card(l, i) for i, l in enumerate(leads))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ScyllaDB GTM Pipeline Report</title>
  <style>
    :root {{
      --primary: #e93b21;
      --bg: #0f172a;
      --surface: #1e293b;
      --surface-2: #2d3f55;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --border: #334155;
      --green: #22c55e;
      --yellow: #f59e0b;
      --red: #ef4444;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.6;
      min-height: 100vh;
    }}
    header {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 1.25rem 2rem;
      display: flex;
      align-items: center;
      gap: 1rem;
    }}
    .logo {{
      width: 36px; height: 36px;
      background: var(--primary);
      border-radius: 8px;
      display: flex; align-items: center; justify-content: center;
      font-weight: 900; font-size: 15px; color: white; flex-shrink: 0;
    }}
    header h1 {{ font-size: 1.1rem; font-weight: 600; }}
    header .generated {{ margin-left: auto; color: var(--text-muted); font-size: 0.8rem; }}
    .container {{ max-width: 1060px; margin: 0 auto; padding: 2rem 1.5rem; }}
    .funnel {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 1rem;
      margin-bottom: 2.5rem;
    }}
    .funnel-stage {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 1.5rem 1rem;
      text-align: center;
      position: relative;
    }}
    .funnel-stage::after {{
      content: '→';
      position: absolute;
      right: -0.65rem;
      top: 50%;
      transform: translateY(-50%);
      color: var(--text-muted);
      font-size: 1.1rem;
      background: var(--bg);
      padding: 0 2px;
    }}
    .funnel-stage:last-child::after {{ display: none; }}
    .funnel-count {{
      font-size: 2.75rem;
      font-weight: 800;
      color: var(--primary);
      line-height: 1;
    }}
    .funnel-label {{
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: var(--text-muted);
      margin-top: 0.4rem;
      font-weight: 600;
    }}
    .funnel-sub {{
      font-size: 0.75rem;
      color: var(--text-muted);
      margin-top: 0.25rem;
    }}
    .section-title {{
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.07em;
      margin-bottom: 1rem;
    }}
    .lead-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      margin-bottom: 0.75rem;
      overflow: hidden;
      transition: border-color 0.15s;
    }}
    .lead-card:hover {{ border-color: var(--surface-2); }}
    .lead-header {{
      display: flex;
      align-items: center;
      padding: 1.1rem 1.25rem;
      gap: 1rem;
      cursor: pointer;
      user-select: none;
    }}
    .lead-header:hover {{ background: rgba(255,255,255,0.03); }}
    .score-badge {{
      width: 52px; height: 52px;
      border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-weight: 800; font-size: 0.95rem;
      flex-shrink: 0;
    }}
    .lead-info {{ flex: 1; min-width: 0; }}
    .lead-name {{ font-weight: 600; font-size: 0.95rem; }}
    .lead-meta {{ color: var(--text-muted); font-size: 0.82rem; margin-top: 0.2rem; }}
    .lead-tags {{ display: flex; gap: 0.4rem; flex-wrap: wrap; margin-top: 0.4rem; }}
    .tag {{
      padding: 0.15rem 0.55rem;
      border-radius: 4px;
      font-size: 0.7rem;
      font-weight: 600;
    }}
    .tag-tech {{ background: rgba(233,59,33,0.12); color: #fc8673; }}
    .tag-signal {{ background: rgba(34,197,94,0.12); color: #4ade80; }}
    .tag-ds-alumni {{ background: rgba(168,85,247,0.12); color: #c084fc; }}
    .tag-active {{ background: rgba(251,191,36,0.12); color: #fbbf24; }}
    .expand-icon {{
      color: var(--text-muted);
      font-size: 1.1rem;
      transition: transform 0.2s;
      flex-shrink: 0;
    }}
    .expand-icon.open {{ transform: rotate(90deg); }}
    .lead-detail {{
      display: none;
      border-top: 1px solid var(--border);
      padding: 1.5rem 1.25rem;
    }}
    .lead-bio {{
      font-size: 0.875rem;
      color: var(--text-muted);
      font-style: italic;
      margin-bottom: 1.25rem;
      line-height: 1.7;
    }}
    .score-breakdown {{
      display: flex;
      gap: 0.75rem;
      margin-bottom: 1.25rem;
      flex-wrap: wrap;
    }}
    .breakdown-item {{
      background: var(--surface-2);
      border-radius: 8px;
      padding: 0.75rem 1rem;
      flex: 1;
      min-width: 110px;
    }}
    .breakdown-label {{
      font-size: 0.67rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600;
    }}
    .breakdown-value {{
      font-size: 1.4rem;
      font-weight: 800;
      color: var(--text);
      margin-top: 0.2rem;
    }}
    .pain-section {{ margin-bottom: 1.25rem; }}
    .pain-section h4 {{
      font-size: 0.75rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 0.5rem;
    }}
    .pain-list {{ list-style: none; }}
    .pain-list li {{
      font-size: 0.85rem;
      color: var(--text);
      padding: 0.3rem 0;
      padding-left: 1rem;
      position: relative;
    }}
    .pain-list li::before {{
      content: '·';
      position: absolute;
      left: 0;
      color: var(--primary);
      font-weight: 700;
    }}
    .messages-section h4 {{
      font-size: 0.75rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 0.75rem;
    }}
    .message-card {{
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem 1.1rem;
      margin-bottom: 0.75rem;
    }}
    .message-type-label {{
      font-size: 0.7rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: var(--primary);
      margin-bottom: 0.6rem;
    }}
    .message-text {{
      font-size: 0.85rem;
      color: var(--text);
      white-space: pre-wrap;
      line-height: 1.75;
    }}
    .no-messages {{ color: var(--text-muted); font-size: 0.85rem; font-style: italic; }}
    footer {{
      text-align: center;
      color: var(--text-muted);
      font-size: 0.75rem;
      padding: 2rem;
      border-top: 1px solid var(--border);
      margin-top: 2rem;
    }}
  </style>
</head>
<body>
  <header>
    <div class="logo">S</div>
    <h1>ScyllaDB GTM Pipeline</h1>
    <span class="generated">Generated {generated_at}</span>
  </header>

  <div class="container">
    <div class="funnel">
      <div class="funnel-stage">
        <div class="funnel-count">{gathered}</div>
        <div class="funnel-label">Gathered</div>
        <div class="funnel-sub">total leads loaded</div>
      </div>
      <div class="funnel-stage">
        <div class="funnel-count">{scored}</div>
        <div class="funnel-label">Scored</div>
        <div class="funnel-sub">leads evaluated</div>
      </div>
      <div class="funnel-stage">
        <div class="funnel-count">{selected}</div>
        <div class="funnel-label">Selected</div>
        <div class="funnel-sub">above threshold</div>
      </div>
      <div class="funnel-stage">
        <div class="funnel-count">{messaged}</div>
        <div class="funnel-label">Messaged</div>
        <div class="funnel-sub">outreach sent</div>
      </div>
    </div>

    <div class="section-title">All Leads — click to expand</div>
    {lead_cards}
  </div>

  <footer>ScyllaDB Hunter · dry_run mode · click any lead to expand messages</footer>

  <script>
    function toggleCard(id) {{
      const detail = document.getElementById(id);
      const icon = document.getElementById('icon_' + id);
      const isOpen = detail.style.display === 'block';
      detail.style.display = isOpen ? 'none' : 'block';
      icon.classList.toggle('open', !isOpen);
    }}
  </script>
</body>
</html>"""
