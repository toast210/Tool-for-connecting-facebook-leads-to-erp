"""ERPNext: URL and headers, the push with duplicate handling, and response classification."""
import json
import re

import requests

from .config import HTTP_TIMEOUT
from .errors import LeadError
from .security import redact


def erp_lead_url(cfg):
    erp = cfg["erpnext"]
    return erp.get("lead_url") or erp["base_url"].rstrip("/") + "/api/resource/Lead"


def erp_headers(cfg):
    erp = cfg["erpnext"]
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if erp.get("api_key") and erp.get("api_secret"):
        headers["Authorization"] = f"token {erp['api_key']}:{erp['api_secret']}"
    headers.update(erp.get("extra_headers") or {})
    return headers


def body_error(j):
    """Detect an error hidden inside an HTTP 200 body (typical for Frappe)."""
    if not isinstance(j, dict):
        return None
    bad = {"error", "failed", "fail", "failure"}
    if j.get("exc") or j.get("exc_type") or j.get("exception"):
        return str(j.get("exc_type") or j.get("exception") or j.get("exc"))[:300]
    if j.get("error"):
        return str(j["error"])[:300]
    if str(j.get("status", "")).lower() in bad or j.get("success") is False:
        return f"status={j.get('status')} message={j.get('message')}"[:300]
    m = j.get("message")
    if isinstance(m, dict) and (str(m.get("status", "")).lower() in bad
                                or m.get("success") is False or m.get("error")):
        return json.dumps(m)[:300]
    return None


TRANSIENT_BODY_RE = re.compile(
    r"lock wait timeout|deadlock|timed out|timeout|temporarily unavailable|try again|TimestampMismatch", re.I)


def server_messages(j):
    """Readable text from Frappe's _server_messages (a JSON string of JSON strings)."""
    raw = j.get("_server_messages") if isinstance(j, dict) else None
    if not raw:
        return ""
    try:
        items = json.loads(raw) if isinstance(raw, str) else raw
        out = []
        for it in items:
            try:
                it = json.loads(it) if isinstance(it, str) else it
            except ValueError:
                pass
            out.append(str(it.get("message") if isinstance(it, dict) else it))
        return " | ".join(out)
    except (ValueError, TypeError):
        return str(raw)


def erp_created_name(text):
    try:
        j = json.loads(text)
    except (ValueError, TypeError):
        return "n/a"
    if isinstance(j, dict):
        for src in (j, j.get("message"), j.get("data")):
            if isinstance(src, dict) and src.get("name"):
                return str(src["name"])
        if isinstance(j.get("message"), str):
            return j["message"][:60]
    return "n/a"


def is_duplicate(status, text, j, id_field):
    if status == 409:
        return True
    low = (text or "").lower()
    if isinstance(j, dict) and str(j.get("exc_type", "")).lower().startswith("duplicate"):
        return True
    return "duplicate" in low and id_field.lower() in low


def classify_erp_http(status):
    if status in (401, 403):
        return "token"
    if status in (408, 429) or status >= 500:
        return "transient"
    return "permanent"


# ---------------------------------------------------------------- ERPNext push
def _erp_request(method, url, cfg, **kw):
    try:
        return requests.request(method, url, headers=erp_headers(cfg), timeout=HTTP_TIMEOUT, **kw)
    except requests.RequestException as e:
        raise LeadError("push", "transient", f"ERPNext request failed: {e}")


def erp_already_exists(leadgen_id, cfg):
    erp = cfg["erpnext"]
    field = erp.get("leadgen_id_field", "leadgen_id")
    url = erp.get("duplicate_check_url") or erp_lead_url(cfg)
    r = _erp_request("GET", url, cfg, params={
        "filters": json.dumps([[field, "=", leadgen_id]]),
        "fields": json.dumps(["name"]), "limit_page_length": 1})
    if r.status_code >= 400:
        raise LeadError("push", classify_erp_http(r.status_code),
                        f"Duplicate check failed (HTTP {r.status_code})", r.status_code, r.text)
    try:
        j = r.json()
    except ValueError:
        return False
    data = j.get("data", j.get("message")) if isinstance(j, dict) else None
    return bool(data) and isinstance(data, list)


def push_lead(leadgen_id, payload, cfg):
    """POST the lead to ERPNext. Returns (http_status, body_text). Raises LeadError."""
    erp = cfg["erpnext"]
    id_field = erp.get("leadgen_id_field", "leadgen_id")
    if erp.get("duplicate_check") and erp_already_exists(leadgen_id, cfg):
        return 200, "duplicate_check: already exists"
    r = _erp_request("POST", erp_lead_url(cfg), cfg, json=payload)
    text = r.text
    try:
        j = r.json()
    except ValueError:
        j = None
    if is_duplicate(r.status_code, text, j, id_field):
        return r.status_code, text            # already created -> treat as success
    if r.status_code >= 400:
        raise LeadError("push", classify_erp_http(r.status_code),
                        f"ERPNext HTTP {r.status_code}: {redact(text, 300)}", r.status_code, text)
    err = body_error(j)
    if err:
        sm = server_messages(j)
        detail = f"{err}; {sm}" if sm else err
        cls = "transient" if TRANSIENT_BODY_RE.search(detail) else "permanent"
        raise LeadError("push", cls, f"ERPNext rejected the lead (HTTP {r.status_code}): {detail}"[:1000],
                        r.status_code, text)
    return r.status_code, text
