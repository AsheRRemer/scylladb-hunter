import logging
import os

import requests

logger = logging.getLogger(__name__)

CONTACTS_UPSERT_URL = "https://api.hubapi.com/crm/v3/objects/contacts/batch/upsert"
EMAILS_URL = "https://api.hubapi.com/crm/v3/objects/emails"
ASSOCIATIONS_URL = "https://api.hubapi.com/crm/v4/objects/emails/{email_id}/associations/contacts/{contact_id}"


def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _upsert_contact(api_key: str, lead: dict) -> str | None:
    """Create or update a HubSpot contact by email. Returns the HubSpot contact ID."""
    email = lead.get("email")
    if not email:
        logger.warning("HubSpot: no email for %s — contact not created", lead["name"])
        return None

    first, *rest = lead["name"].split(" ", 1)
    last = rest[0] if rest else ""

    payload = {
        "inputs": [
            {
                "idProperty": "email",
                "properties": {
                    "email": email,
                    "firstname": first,
                    "lastname": last,
                    "jobtitle": lead.get("title", ""),
                    "company": lead.get("company", ""),
                    "city": lead.get("location", "").split(",")[0].strip(),
                    "hs_lead_status": "IN_PROGRESS",
                },
            }
        ]
    }

    resp = requests.post(CONTACTS_UPSERT_URL, json=payload, headers=_headers(api_key), timeout=10)
    resp.raise_for_status()
    contact_id = resp.json()["results"][0]["id"]
    logger.info("HubSpot: upserted contact %s → id %s", lead["name"], contact_id)
    return contact_id


def _log_email(api_key: str, lead: dict, subject: str, body: str,
               sender_name: str, sender_email: str) -> str | None:
    """Create an email engagement in HubSpot. Returns the HubSpot email object ID."""
    payload = {
        "properties": {
            "hs_email_direction": "EMAIL",
            "hs_email_status": "SENT",
            "hs_email_subject": subject,
            "hs_email_text": body,
            "hs_email_sender_email": sender_email,
            "hs_email_sender_firstname": sender_name.split()[0],
            "hs_email_to_email": lead.get("email", ""),
            "hs_email_to_firstname": lead.get("first_name", ""),
            "hs_email_to_lastname": lead.get("last_name", ""),
            "hs_timestamp": None,  # HubSpot defaults to now
        }
    }

    resp = requests.post(EMAILS_URL, json=payload, headers=_headers(api_key), timeout=10)
    resp.raise_for_status()
    email_id = resp.json()["id"]
    logger.info("HubSpot: logged email object id %s for %s", email_id, lead["name"])
    return email_id


def _associate_email_to_contact(api_key: str, email_id: str, contact_id: str):
    url = ASSOCIATIONS_URL.format(email_id=email_id, contact_id=contact_id)
    payload = [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 210}]
    resp = requests.put(url, json=payload, headers=_headers(api_key), timeout=10)
    resp.raise_for_status()
    logger.debug("HubSpot: associated email %s → contact %s", email_id, contact_id)


def log_contact(cfg: dict, lead: dict):
    """
    Upsert the lead as a contact in HubSpot.

    Call this from trigger.py when a lead is selected (score above threshold).
    No-op if no API key is configured.
    """
    api_key = (
        cfg.get("hubspot", {}).get("api_key")
        or os.environ.get("HUBSPOT_API_KEY", "")
    )
    if not api_key:
        logger.debug("HubSpot: no API key configured — skipping contact log for %s", lead["name"])
        return

    try:
        _upsert_contact(api_key, lead)
    except requests.HTTPError as e:
        logger.error("HubSpot API error for %s: %s", lead["name"], e)
    except Exception as e:
        logger.error("HubSpot unexpected error for %s: %s", lead["name"], e)


def log_outbound_email(cfg: dict, lead: dict, subject: str, body: str):
    """
    Upsert the contact in HubSpot and log the sent email against them.

    Call this from trigger.py after generating an email or email_followup message.
    No-op if no API key is configured.
    """
    api_key = (
        cfg.get("hubspot", {}).get("api_key")
        or os.environ.get("HUBSPOT_API_KEY", "")
    )
    if not api_key:
        logger.debug("HubSpot: no API key configured — skipping CRM log for %s", lead["name"])
        return

    sender = cfg.get("sender", {})
    sender_name = sender.get("name", "")
    sender_email = sender.get("email", "")

    try:
        contact_id = _upsert_contact(api_key, lead)
        if not contact_id:
            return
        email_id = _log_email(api_key, lead, subject, body, sender_name, sender_email)
        if email_id:
            _associate_email_to_contact(api_key, email_id, contact_id)
    except requests.HTTPError as e:
        logger.error("HubSpot API error for %s: %s", lead["name"], e)
    except Exception as e:
        logger.error("HubSpot unexpected error for %s: %s", lead["name"], e)
