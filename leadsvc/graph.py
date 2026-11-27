"""Graph API: fetch a lead and classify Graph errors."""
import hashlib
import hmac

import requests

from .config import DEFAULT_GRAPH_FIELDS, GRAPH_TOKEN_CODES, GRAPH_TRANSIENT_CODES, HTTP_TIMEOUT
from .errors import LeadError


def classify_graph(status, j):
    err = j.get("error") if isinstance(j, dict) else None
    code = err.get("code") if isinstance(err, dict) else None
    msg = err.get("message") if isinstance(err, dict) else None
    desc = f"Graph API error (HTTP {status}, code {code}): {msg}"
    if isinstance(code, int):
        if code in GRAPH_TOKEN_CODES:
            return "token", desc
        if code in GRAPH_TRANSIENT_CODES or err.get("is_transient"):
            return "transient", desc
    if status in (408, 429) or status >= 500:
        return "transient", desc
    return "permanent", desc


# ---------------------------------------------------------------- graph fetch
def fetch_lead(row, cfg):
    page = cfg["pages"].get(str(row["page_id"]))
    if not page or not page.get("page_access_token"):
        raise LeadError("fetch", "permanent", f"No page_access_token configured for page {row['page_id']}")
    fields = cfg.get("graph_lead_fields") or DEFAULT_GRAPH_FIELDS
    if isinstance(fields, list):
        fields = ",".join(fields)
    base = cfg.get("graph_base_url", "https://graph.facebook.com").rstrip("/")
    url = f"{base}/{cfg.get('graph_api_version', 'v26.0')}/{row['leadgen_id']}"
    try:
        token = page["page_access_token"]
        proof = hmac.new(cfg["app_secret"].encode(), token.encode(), hashlib.sha256).hexdigest()
        r = requests.get(url, params={"fields": fields, "access_token": token, "appsecret_proof": proof},
                         timeout=HTTP_TIMEOUT)
    except requests.RequestException as e:
        raise LeadError("fetch", "transient", f"Graph request failed: {e}")
    try:
        j = r.json()
    except ValueError:
        j = None
    if r.status_code >= 400 or (isinstance(j, dict) and "error" in j):
        cls, desc = classify_graph(r.status_code, j)
        raise LeadError("fetch", cls, desc, r.status_code, r.text)
    if not isinstance(j, dict):
        raise LeadError("fetch", "transient", "Graph returned a non-JSON body", r.status_code, r.text)
    return j
