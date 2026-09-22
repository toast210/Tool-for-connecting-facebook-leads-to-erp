import os
import json
import logging
from flask import Flask, request, jsonify
import requests

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fb-leads-erpnext")

app = Flask(__name__)

CREDENTIALS_PATH = os.environ.get("CREDENTIALS_PATH", "credentials.json")

with open(CREDENTIALS_PATH, "r") as f:
    CREDS = json.load(f)

VERIFY_TOKEN = CREDS["verify_token"]
GRAPH_VERSION = CREDS.get("graph_api_version", "v26.0")

ERP_BASE_URL = CREDS["erpnext"]["base_url"]
ERP_API_KEY = CREDS["erpnext"]["api_key"]
ERP_API_SECRET = CREDS["erpnext"]["api_secret"]

PAGES = CREDS["pages"]

# Top-level fields to request from Graph API (not inside field_data)
TOP_LEVEL_FIELDS = "field_data,campaign_name,adset_name,ad_name,platform,created_time"


@app.route("/webhook/facebook-leads", methods=["GET"])
def verify_webhook():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        log.info("Webhook verified.")
        return challenge, 200
    return "Forbidden", 403


@app.route("/webhook/facebook-leads", methods=["POST"])
def receive_lead():
    payload = request.get_json(silent=True) or {}

    entries = payload.get("entry", [])
    processed = 0
    failed = 0

    for entry in entries:
        page_id = entry.get("id")

        page_config = PAGES.get(page_id)
        if not page_config:
            log.warning(
                "Lead from unrecognized Page ID %s -- skipping.",
                page_id,
            )
            failed += 1
            continue

        for change in entry.get("changes", []):
            leadgen_id = change.get("value", {}).get("leadgen_id")
            if not leadgen_id:
                continue

            try:
                fields = fetch_lead_from_facebook(leadgen_id, page_config)
                push_lead_to_erpnext(fields, page_config)
                processed += 1
            except Exception:
                log.exception(
                    "Failed to process lead %s (page=%s / %s) -- continuing.",
                    leadgen_id,
                    page_id,
                    page_config.get("name"),
                )
                failed += 1
                continue

    log.info("Webhook batch complete: %d processed, %d failed.", processed, failed)
    return jsonify({"status": "received", "processed": processed, "failed": failed}), 200


def fetch_lead_from_facebook(leadgen_id: str, page_config: dict) -> dict:
    """Fetch lead from Graph API and return a flat dict of all fields."""
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{leadgen_id}"
    resp = requests.get(
        url,
        params={
            "access_token": page_config["page_access_token"],
            "fields": TOP_LEVEL_FIELDS,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    fields = {}

    # Extract field_data (form answers)
    for f in data.get("field_data", []):
        values = f.get("values") or [""]
        fields[f["name"]] = values[0]

    # Merge top-level metadata (campaign, adset, ad, platform, created_time)
    for key in ("campaign_name", "adset_name", "ad_name", "platform", "created_time"):
        if data.get(key):
            fields[key] = data[key]

    return fields


def push_lead_to_erpnext(fields: dict, page_config: dict):
    """Map Meta fields to ERPNext Lead doctype and create the record."""
    field_map = page_config.get("field_map", {})
    source = page_config.get("source", "Facebook Lead Ads")

    payload = {"lead_source": source}

    for fb_field, erp_field in field_map.items():
        if fb_field in fields:
            payload[erp_field] = fields[fb_field]

    # Fallback: ensure lead_name is always set
    payload.setdefault("lead_name", fields.get("full_name", "Facebook Lead"))

    # Store raw data for debugging
    payload["lead_raw_data"] = json.dumps(fields, ensure_ascii=False)

    resp = requests.post(
        f"{ERP_BASE_URL}/api/resource/Lead",
        json=payload,
        headers={
            "Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}",
            "Content-Type": "application/json",
        },
        timeout=15,
    )

    if resp.status_code >= 400:
        log.error(
            "ERPNext rejected lead (page=%s, status=%d): %s",
            page_config.get("name"),
            resp.status_code,
            resp.text,
        )
        resp.raise_for_status()

    created_name = resp.json()["data"]["name"]
    log.info(
        "Lead created in ERPNext: %s (page=%s, source=%s)",
        created_name,
        page_config.get("name"),
        source,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)