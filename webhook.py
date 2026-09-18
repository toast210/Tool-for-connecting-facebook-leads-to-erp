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
GRAPH_VERSION = CREDS.get("graph_api_version", "v19.0")

ERP_BASE_URL = CREDS["erpnext"]["base_url"]
ERP_API_KEY = CREDS["erpnext"]["api_key"]
ERP_API_SECRET = CREDS["erpnext"]["api_secret"]

PAGES = CREDS["pages"]  # keyed by Facebook Page ID


@app.route("/webhook/facebook-leads", methods=["GET"])
def verify_webhook():
    """Meta calls this once to verify the endpoint (shared across all Pages)."""
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        log.info("Webhook verified.")
        return challenge, 200
    return "Forbidden", 403


@app.route("/webhook/facebook-leads", methods=["POST"])
def receive_lead():
    """Meta POSTs here for leads from ANY subscribed Page."""
    payload = request.get_json(silent=True) or {}

    try:
        for entry in payload.get("entry", []):
            page_id = entry.get("id")
            page_config = PAGES.get(page_id)

            if not page_config:
                log.warning("Lead from unrecognized Page ID %s -- skipping. "
                            "Add it to credentials.json if this Page is expected.", page_id)
                continue

            for change in entry.get("changes", []):
                leadgen_id = change.get("value", {}).get("leadgen_id")
                if not leadgen_id:
                    continue

                fields = fetch_lead_from_facebook(leadgen_id, page_config)
                push_lead_to_erpnext(fields, page_config)
    except Exception as e:
        log.exception("Error processing lead webhook: %s", e)
        # TODO: write failed payloads to a dead-letter file/table for retry

    return jsonify({"status": "received"}), 200


def fetch_lead_from_facebook(leadgen_id: str, page_config: dict) -> dict:
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{leadgen_id}"
    resp = requests.get(
        url,
        params={"access_token": page_config["page_access_token"]},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()

    fields = {}
    for f in data.get("field_data", []):
        values = f.get("values") or [""]
        fields[f["name"]] = values[0]
    return fields


def push_lead_to_erpnext(fields: dict, page_config: dict):
    field_map = page_config.get("field_map", {})
    source = page_config.get("source", "Facebook Lead Ads")

    payload = {"source": source}
    for fb_field, erp_field in field_map.items():
        if fb_field in fields:
            payload[erp_field] = fields[fb_field]

    payload.setdefault("lead_name", fields.get("full_name", "Facebook Lead"))
    payload["notes"] = json.dumps(fields)

    resp = requests.post(
        f"{ERP_BASE_URL}/api/resource/Lead",
        json=payload,
        headers={
            "Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}",
            "Content-Type": "application/json",
        },
        timeout=10,
    )

    if resp.status_code >= 400:
        log.error("ERPNext rejected lead (page=%s): %s", page_config.get("name"), resp.text)
        resp.raise_for_status()

    created_name = resp.json()["data"]["name"]
    log.info("Lead created in ERPNext: %s (page=%s, source=%s)",
              created_name, page_config.get("name"), source)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)