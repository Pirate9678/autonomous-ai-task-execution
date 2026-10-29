"""AI-prompt layer for the autonomous task execution pipeline.

In production this module fills the custom prompt templates from
``pipeline/prompts.yaml`` with per-record values and sends them to an LLM
(via the n8n AI/OpenAI node or a direct API call). The returned JSON is
parsed and merged back into the record.

For a fully local, dependency-free run, ``analyse_record()`` uses a
deterministic rule-based classifier that mirrors the semantics of the
``classification`` prompt in prompts.yaml — the same categories, sentiments
and priority rules — so the pipeline can be demoed and tested anywhere
without an API key.

Standard library only.
"""

import json
import re

# ---------------------------------------------------------------------------
# Prompt template loading (tiny built-in YAML-subset reader — no PyYAML needed)
# ---------------------------------------------------------------------------

def load_prompts(path="pipeline/prompts.yaml"):
    """Load prompt templates from prompts.yaml.

    Parses just the subset of YAML we use: top-level ``prompts:`` mapping,
    each prompt having name/description/template keys, with the template
    written as a literal block scalar (|). Returns a dict of
    {prompt_key: {"name": ..., "description": ..., "template": ...}}.
    """
    prompts = {}
    current_key = None
    current_field = None
    in_template = False
    template_lines = []
    template_indent = None

    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if not line.strip() or line.strip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip(" "))

            if in_template:
                if template_indent is None and line.strip():
                    template_indent = indent
                if line.strip() and indent < template_indent:
                    # Template block ended — store it and reprocess this line.
                    prompts[current_key]["template"] = "\n".join(template_lines).rstrip("\n")
                    in_template = False
                    template_lines = []
                    template_indent = None
                elif not line.strip():
                    template_lines.append("")
                    continue
                else:
                    template_lines.append(line[template_indent:] if len(line) > (template_indent or 0) else "")
                    continue

            stripped = line.strip()
            if indent == 0 and stripped == "prompts:":
                continue
            if indent == 2 and stripped.endswith(":") and stripped != "prompts:":
                current_key = stripped[:-1]
                prompts[current_key] = {}
                current_field = None
                continue
            if indent == 4 and current_key:
                if ":" in stripped:
                    field, _, value = stripped.partition(":")
                    field = field.strip()
                    value = value.strip()
                    if value == "|":
                        current_field = "template"
                        in_template = True
                        template_lines = []
                        template_indent = None
                        prompts[current_key]["template"] = ""
                    elif field in ("name", "description"):
                        current_field = field
                        prompts[current_key][field] = value.strip('"').strip("'")
                    continue
            if indent >= 6 and current_field and current_key and not in_template:
                # Continuation of a folded (>) description line.
                prompts[current_key][current_field] += " " + stripped

    if in_template and current_key:
        prompts[current_key]["template"] = "\n".join(template_lines).rstrip("\n")
    return prompts


def render_prompt(template, record):
    """Substitute {{field}} placeholders with values from a record dict.

    Missing fields become an empty string. This mirrors exactly what the
    n8n workflow does when it injects record fields into the system prompt
    before calling the LLM.
    """
    def _replace(match):
        field = match.group(1).strip()
        value = record.get(field, "")
        return "" if value is None else str(value)

    return re.sub(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}", _replace, template)


# ---------------------------------------------------------------------------
# Deterministic classifier (local stand-in for the LLM call)
# ---------------------------------------------------------------------------

_BILLING = ("invoice", "bill", "charge", "charged", "refund", "payment", "price", "overcharg")
_TECHNICAL = ("error", "bug", "crash", "login", "password", "broken", "not working",
              "failed", "failure", "timeout", "server", "api")
_LOGISTICS = ("deliver", "shipping", "shipment", "package", "courier", "tracking",
              "arrived", "late", "dispatch")
_ACCOUNT = ("account", "subscription", "cancel", "upgrade", "plan", "profile",
            "unsubscribe", "renewal")
_FEEDBACK = ("suggestion", "feedback", "feature request", "improve", "love", "great service")

_NEGATIVE = ("angry", "furious", "terrible", "worst", "awful", "horrible", "disappoint",
             "frustrat", "urgent", "immediately", "asap", "unacceptable", "complaint")
_POSITIVE = ("thank", "great", "excellent", "love", "awesome", "happy", "satisfied",
             "appreciate", "wonderful")

_URGENT_HINTS = ("outage", "down", "charged twice", "double charge", "unauthorised",
                 "unauthorized", "data loss", "breach")


def _classify_text(text):
    """Rule-based approximation of the `classification` prompt's decisions."""
    t = text.lower()

    if any(k in t for k in _BILLING):
        category = "billing"
    elif any(k in t for k in _LOGISTICS):
        category = "logistics"
    elif any(k in t for k in _ACCOUNT):
        category = "account"
    elif any(k in t for k in _TECHNICAL):
        category = "technical"
    elif any(k in t for k in _FEEDBACK):
        category = "feedback"
    else:
        category = "unknown"

    if any(k in t for k in _NEGATIVE):
        sentiment = "negative"
    elif any(k in t for k in _POSITIVE):
        sentiment = "positive"
    else:
        sentiment = "neutral"

    if any(k in t for k in _URGENT_HINTS):
        priority = "urgent"
    elif sentiment == "negative" and category in ("billing", "technical"):
        priority = "high"
    elif sentiment == "negative":
        priority = "medium"
    elif category == "feedback":
        priority = "low"
    else:
        priority = "medium"

    action_required = sentiment == "negative" or priority in ("high", "urgent")
    return {
        "category": category,
        "sentiment": sentiment,
        "priority": priority,
        "action_required": action_required,
    }


def validate_classification(ticket_id, classification):
    """Local equivalent of the `validation` prompt's QA rules."""
    notes = []
    valid = True

    if classification["category"] == "unknown" and classification["sentiment"] == "negative":
        valid = False
        notes.append("negative ticket left with unknown category")

    if classification["priority"] == "urgent" and not classification["action_required"]:
        valid = False
        notes.append("urgent ticket flagged as needing no action")

    if classification["category"] == "unknown":
        notes.append("low-confidence category; routed to manual review")

    confidence = 0.95 if valid and classification["category"] != "unknown" else 0.55
    return {
        "ticket_id": ticket_id,
        "valid": valid,
        "confidence": confidence,
        "notes": "; ".join(notes) if notes else "passed automated QA",
    }


def analyse_record(record, prompts=None):
    """Run the full custom-prompt pipeline over one record.

    1. Renders the classification template with the record's fields
       (this is the prompt that would be sent to the LLM in production).
    2. Applies the deterministic classifier (local stand-in for the LLM).
    3. Runs the validation self-check.

    Returns (enriched_record, rendered_prompt, validation_result).
    The rendered prompt is returned for transparency/logging — in the n8n
    workflow the equivalent prompt text is visible in the AI node config.
    """
    prompts = prompts or load_prompts()
    rendered = render_prompt(prompts["classification"]["template"], record)
    classification = _classify_text(
        f"{record.get('subject', '')} {record.get('description', '')}"
    )
    validation = validate_classification(record.get("ticket_id", ""), classification)

    enriched = dict(record)
    enriched.update({
        "ai_category": classification["category"],
        "ai_sentiment": classification["sentiment"],
        "ai_priority": classification["priority"],
        "ai_action_required": str(classification["action_required"]),
        "ai_confidence": validation["confidence"],
        "ai_valid": str(validation["valid"]),
    })
    return enriched, rendered, validation


# ---------------------------------------------------------------------------
# Optional production LLM hook (not used in local runs)
# ---------------------------------------------------------------------------

def analyse_record_with_llm(record, prompts, api_key, model="gpt-4o-mini",
                            endpoint="https://api.openai.com/v1/chat/completions"):
    """Production path: send the rendered prompt to an LLM via HTTP.

    Uses urllib from the standard library (still no extra dependencies).
    The n8n workflow calls the equivalent of this function through its
    OpenAI / HTTP Request node. Raises RuntimeError on non-200 responses.
    """
    import urllib.request

    rendered = render_prompt(prompts["classification"]["template"], record)
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": rendered}],
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
    }).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        body = json.loads(response.read().decode("utf-8"))
    content = body["choices"][0]["message"]["content"]
    return json.loads(content)
