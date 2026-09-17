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

FB_VERIFY_TOKEN = CREDS["facebook"]["verify_token"]
FB_PAGE_ACCESS_TOKEN = CREDS["facebook"]["page_access_token"]
FB_GRAPH_VERSION = CREDS["facebook"].get("graph_api_version", "v19.0")

ERP_BASE_URL = CREDS["erpnext"]["base_url"]
ERP_API_KEY = CREDS["erpnext"]["api_key"]
ERP_API_SECRET = CREDS["erpnext"]["api_secret"]

SOURCE = CREDS.get("source", "Facebook Lead Ads")
FIELD_MAP = CREDS.get("field_map", {})


@app.route("/webhook/facebook-leads", methods=["GET"])
def verify_webhook():
    """Meta calls this once to verify the endpoint."""
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == FB_VERIFY_TOKEN:
        log.info("Webhook verified.")
        return challenge, 200
    return "Forbidden", 403


@app.route("/webhook/facebook-leads", methods=["POST"])
def receive_lead():
    """Meta POSTs here whenever a new lead is submitted, from ANY form."""
    payload = request.get_json(silent=True) or {}

    try:
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                leadgen_id = change.get("value", {}).get("leadgen_id")
                if not leadgen_id:
                    continue

                fields = fetch_lead_from_facebook(leadgen_id)
                push_lead_to_erpnext(fields)
    except Exception as e:
        log.exception("Error processing lead webhook: %s", e)
        # TODO: write failed payloads to a dead-letter file/table for retry

    return jsonify({"status": "received"}), 200


def fetch_lead_from_facebook(leadgen_id: str) -> dict:
    url = f"https://graph.facebook.com/{FB_GRAPH_VERSION}/{leadgen_id}"
    resp = requests.get(url, params={"access_token": FB_PAGE_ACCESS_TOKEN}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    fields = {}
    for f in data.get("field_data", []):
        values = f.get("values") or [""]
        fields[f["name"]] = values[0]
    return fields


def push_lead_to_erpnext(fields: dict):
    payload = {"source": SOURCE}
    for fb_field, erp_field in FIELD_MAP.items():
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
        log.error("ERPNext rejected lead: %s", resp.text)
        resp.raise_for_status()

    created_name = resp.json()["data"]["name"]
    log.info("Lead created in ERPNext: %s", created_name)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)