"""Facebook Lead Ads -> ERPNext webhook.

Receives Meta leadgen webhooks, queues the lead ID in SQLite, and a background
worker fetches the full lead from the Graph API and POSTs it to ERPNext, with
retries. See README.md for setup.
"""
import hashlib
import hmac
import json
import logging
import os
import random
import re
import socket
import sqlite3
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests
from flask import Flask, jsonify, request

# ---------------------------------------------------------------- constants
MAX_RETRIES = 5                 # retries after the first attempt; failure #6 is final
BACKOFF_BASE_SECONDS = 60       # 1, 2, 4, 8, 16 minutes
BATCH_SIZE = 20                 # leads handled per worker loop
HTTP_TIMEOUT = 15               # seconds
STALE_PROCESSING_SECONDS = 300
DONE_RETENTION_DAYS = 30
DEAD_RETENTION_DAYS = 90
WORKER_IDLE_SECONDS = 3
MAINTENANCE_EVERY_SECONDS = 60
HEARTBEAT_ALIVE_SECONDS = 30

CREDENTIALS_PATH = os.environ.get("CREDENTIALS_PATH", "credentials.json")
DATABASE_PATH = os.environ.get("DATABASE_PATH", "leads.db")
PORT = int(os.environ.get("PORT", "3000"))

DEFAULT_GRAPH_FIELDS = (
    "id,created_time,ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,"
    "form_id,field_data,is_organic,platform,custom_disclaimer_responses"
)
AD_FIELDS = ["created_time", "ad_id", "ad_name", "adset_id", "adset_name",
             "campaign_id", "campaign_name", "form_id", "platform", "is_organic"]

# Output name -> form question keys (matched case-insensitively, ignoring spaces/punctuation).
# Override or extend with "payload.field_mapping" in credentials.json.
DEFAULT_FIELD_MAPPING = {
    "Name": ["full_name", "name", "your_name", "customer_name"],
    "Phone": ["phone_number", "phone", "mobile", "mobile_number", "work_phone_number", "whatsapp_number"],
    "Email": ["email", "email_address", "work_email"],
}
# Graph/webhook field -> output name. Order matters: the first three are sent right after the answers.
DEFAULT_META_NAMES = {
    "ad_name": "Ad Name", "adset_name": "Adset Name", "campaign_name": "Campaign",
    "ad_id": "Ad ID", "adset_id": "Adset ID", "campaign_id": "Campaign ID", "form_id": "Form ID",
    "created_time": "Created Time", "platform": "Platform", "is_organic": "Is Organic",
    "page_id": "Page ID", "lead_source": "Lead Source",
}
PRIMARY_META = ("ad_name", "adset_name", "campaign_name")

GRAPH_TOKEN_CODES = {10, 102, 190} | set(range(200, 300))
GRAPH_TRANSIENT_CODES = {1, 2, 4, 17, 32, 341, 613} | set(range(80000, 80015))

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")
log = logging.getLogger("fb-leads")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1_000_000   # webhooks are tiny; reject anything large


# ---------------------------------------------------------------- time utils
_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def now_dt():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime(_TS_FMT)


def now_iso():
    return iso(now_dt())


def parse_iso(s):
    return datetime.strptime(s, _TS_FMT).replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------- config
_cfg_lock = threading.Lock()
_cfg_cache = {"mtime": None, "data": None}


def _load_config_file():
    try:
        with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        raise RuntimeError(f"Cannot read/parse {CREDENTIALS_PATH}: {e}")
    if not isinstance(data, dict):
        raise RuntimeError("credentials.json must contain a JSON object")
    for key in ("verify_token", "app_secret", "admin_api_key"):
        if not data.get(key):
            raise RuntimeError(f"credentials.json: '{key}' is required")
    erp = data.get("erpnext") or {}
    if not (erp.get("lead_url") or erp.get("base_url")):
        raise RuntimeError("credentials.json: erpnext.lead_url (or base_url) is required")
    if not isinstance(data.get("pages"), dict) or not data["pages"]:
        raise RuntimeError("credentials.json: 'pages' must contain at least one Page")
    validate_payload_cfg(data.get("payload") or {})
    return data


def get_config():
    """Load credentials.json, reloading automatically when the file changes.

    If the file is edited into something invalid while the app is running, the last
    good settings stay in use (and the error is logged) instead of breaking every request.
    """
    with _cfg_lock:
        try:
            mtime = os.path.getmtime(CREDENTIALS_PATH)
        except OSError as e:
            if _cfg_cache["data"] is not None:
                return _cfg_cache["data"]
            raise RuntimeError(f"Cannot read {CREDENTIALS_PATH}: {e}")
        if mtime != _cfg_cache["mtime"] and mtime != _cfg_cache.get("bad_mtime"):
            try:
                _cfg_cache.update(mtime=mtime, data=_load_config_file(), bad_mtime=None)
            except RuntimeError as e:
                if _cfg_cache["data"] is None:
                    raise
                log.error("credentials.json changed but is invalid (%s); keeping the previous settings", e)
                _cfg_cache["bad_mtime"] = mtime
        return _cfg_cache["data"]


def validate_payload_cfg(p):
    def bad(msg):
        raise RuntimeError(f"credentials.json payload: {msg}")
    if not isinstance(p, dict):
        bad("must be an object")
    fm = p.get("field_mapping", {})
    if not isinstance(fm, dict) or not all(
            isinstance(k, str) and (isinstance(v, str) or (isinstance(v, list) and all(isinstance(x, str) for x in v)))
            for k, v in fm.items()):
        bad("field_mapping must map an output name to a form key or a list of form keys")
    for key in ("drop_fields", "required_fields", "phone_fields", "email_fields", "send_meta_fields"):
        if key in p and not (isinstance(p[key], list) and all(isinstance(x, str) for x in p[key])):
            bad(f"{key} must be a list of strings")
    for key in ("meta_field_names", "static_fields"):
        if key in p and not isinstance(p[key], dict):
            bad(f"{key} must be an object")
    unknown = [k for k in p.get("send_meta_fields", []) if k not in DEFAULT_META_NAMES]
    if unknown:
        bad(f"send_meta_fields has unknown entries {unknown}; allowed: {list(DEFAULT_META_NAMES)}")
    unknown = [k for k in p.get("meta_field_names", {}) if k not in DEFAULT_META_NAMES]
    if unknown:
        bad(f"meta_field_names has unknown keys {unknown}; allowed: {list(DEFAULT_META_NAMES)}")


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


# ---------------------------------------------------------------- redaction
_REDACT_PATTERNS = [
    (re.compile(r"(access_token=)[^&\s\"']+", re.I), r"\1[REDACTED]"),
    (re.compile(r"(appsecret_proof=)[^&\s\"']+", re.I), r"\1[REDACTED]"),
    (re.compile(r"(\"access_token\"\s*:\s*\")[^\"]+", re.I), r"\1[REDACTED]"),
    (re.compile(r"((?:token|bearer)\s+)[A-Za-z0-9_\-.:]{8,}", re.I), r"\1[REDACTED]"),
]


def _known_secrets():
    try:
        cfg = get_config()
    except Exception:
        return []
    vals = [cfg.get("app_secret"), cfg.get("admin_api_key"), cfg.get("verify_token")]
    for p in cfg.get("pages", {}).values():
        vals.append(p.get("page_access_token"))
    erp = cfg.get("erpnext", {})
    vals += [erp.get("api_key"), erp.get("api_secret")]
    vals += list((erp.get("extra_headers") or {}).values())
    return [v for v in vals if isinstance(v, str) and len(v) >= 6]


def redact(text, limit=2000):
    if text is None:
        return None
    text = str(text)
    for secret in _known_secrets():
        text = text.replace(secret, "[REDACTED]")
    for pat, repl in _REDACT_PATTERNS:
        text = pat.sub(repl, text)
    return text[:limit]


# ---------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id TEXT NOT NULL UNIQUE,
    page_id TEXT,
    form_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    requeues INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    locked_at TEXT,
    last_error TEXT,
    last_error_class TEXT,
    last_error_stage TEXT,
    payload TEXT,
    graph_data TEXT,
    webhook_data TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    done_at TEXT,
    dead_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_leads_queue ON leads(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_leads_page ON leads(page_id);
CREATE TABLE IF NOT EXISTS lead_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    stage TEXT NOT NULL,
    ok INTEGER NOT NULL,
    error_class TEXT,
    http_status INTEGER,
    response_body TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_lead ON lead_attempts(leadgen_id);
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    pid INTEGER,
    last_seen TEXT NOT NULL
);
"""


def db():
    conn = sqlite3.connect(DATABASE_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    with closing(db()) as conn:
        conn.executescript(SCHEMA)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(leads)")}
        if "graph_data" not in cols:      # database created by an older version
            conn.execute("ALTER TABLE leads ADD COLUMN graph_data TEXT")


def record_attempt(conn, leadgen_id, attempt_no, stage, ok, error_class=None,
                   http_status=None, body=None, error=None):
    conn.execute(
        "INSERT INTO lead_attempts(leadgen_id,attempt_no,stage,ok,error_class,http_status,"
        "response_body,error,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (leadgen_id, attempt_no, stage, 1 if ok else 0, error_class, http_status,
         redact(body), redact(error, 1000), now_iso()))


# ---------------------------------------------------------------- errors
class LeadError(Exception):
    def __init__(self, stage, cls, message, http_status=None, body=None):
        super().__init__(message)
        self.stage, self.cls = stage, cls
        self.message, self.http_status, self.body = message, http_status, body


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


def norm_key(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


def pretty_name(key):
    words = re.sub(r"[_\s]+", " ", str(key)).strip().split()
    return " ".join(w[:1].upper() + w[1:] for w in words) or str(key)


def clean_phone(v):
    v = v.strip()
    if v.lower().startswith("p:"):            # Meta sometimes prefixes numbers with "p:"
        v = v[2:]
    cleaned = re.sub(r"[^\d+]", "", v)
    return cleaned if re.search(r"\d", cleaned) else v


def payload_settings(cfg):
    p = cfg.get("payload") or {}
    mapping = {k: list(v) for k, v in DEFAULT_FIELD_MAPPING.items()}
    for out_name, src in (p.get("field_mapping") or {}).items():
        mapping[out_name] = [src] if isinstance(src, str) else list(src)
    names = dict(DEFAULT_META_NAMES)
    names.update(p.get("meta_field_names") or {})
    return {
        "mapping": mapping,
        "meta_names": names,
        "send_meta": p.get("send_meta_fields", list(DEFAULT_META_NAMES)),
        "include_unmapped": p.get("include_unmapped_answers", True),
        "drop": {norm_key(k) for k in p.get("drop_fields", [])},
        "required": p.get("required_fields", []),
        "phone_fields": set(p.get("phone_fields", ["Phone"])),
        "email_fields": set(p.get("email_fields", ["Email"])),
        "normalize": p.get("normalize_values", True),
        "static": p.get("static_fields") or {},
    }


def build_payload(row, graph, cfg):
    """Turn a Graph lead into the flat JSON sent to ERPNext (field matching happens here)."""
    ps = payload_settings(cfg)
    wh = json.loads(row["webhook_data"] or "{}")
    page = cfg["pages"].get(str(row["page_id"]), {})
    id_field = cfg["erpnext"].get("leadgen_id_field", "leadgen_id")

    answers = {}        # normalised key -> (original key, value)
    for f in graph.get("field_data") or []:
        name = f.get("name")
        if name:
            answers[norm_key(name)] = (name, ", ".join(str(v) for v in (f.get("values") or [])).strip())

    used, out = set(), {}
    for out_name, sources in ps["mapping"].items():
        val = ""
        for src in sources:
            k = norm_key(src)
            if k in answers:
                used.add(k)
                if not val:
                    val = answers[k][1]
        out[out_name] = val
    if "Name" in out and not out["Name"]:       # build a name from first/last name when there is no full name
        first = answers.get("first_name", ("", ""))[1]
        last = answers.get("last_name", ("", ""))[1]
        if first or last:
            out["Name"] = f"{first} {last}".strip()
            used.update(k for k in ("first_name", "last_name") if k in answers)

    if ps["normalize"]:
        for k, v in out.items():
            if k in ps["phone_fields"] and v:
                out[k] = clean_phone(v)
            elif k in ps["email_fields"] and v:
                out[k] = v.strip().lower()
            elif k == "Name" and v:
                out[k] = " ".join(v.split())

    # ad / page data
    vals = {k: graph.get(k) for k in AD_FIELDS}
    for k, fb in (("ad_id", wh.get("ad_id")), ("adset_id", wh.get("adgroup_id")),
                  ("form_id", wh.get("form_id")), ("created_time", wh.get("created_time"))):
        if vals.get(k) is None:
            vals[k] = fb
    vals["page_id"] = str(row["page_id"])
    vals["lead_source"] = page.get("source", "Facebook Lead Ads")
    meta = {}
    for k in ps["send_meta"]:
        v = vals.get(k)
        meta[k] = ("" if v is None else v)

    def put_meta(keys):
        for k in keys:
            if k in meta:
                out[ps["meta_names"][k]] = meta[k]
    put_meta(PRIMARY_META)

    reserved = set(out) | {ps["meta_names"][k] for k in meta} | {id_field}
    if ps["include_unmapped"]:
        for nk, (orig, val) in answers.items():
            if nk in used or nk in ps["drop"] or not val:
                continue
            name = pretty_name(orig)
            if name in reserved:
                name += " (form)"
            out[name] = val
            reserved.add(name)
    for d in graph.get("custom_disclaimer_responses") or []:
        key = d.get("checkbox_key")
        if key:
            out[f"Disclaimer {pretty_name(key)}"] = d.get("status", d.get("value", ""))

    put_meta([k for k in meta if k not in PRIMARY_META])
    out.update(ps["static"])
    out[id_field] = row["leadgen_id"]
    return out


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


# ---------------------------------------------------------------- queue processing
def claim_one(conn):
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = now_iso()
        row = conn.execute(
            "SELECT * FROM leads WHERE status='pending' AND next_attempt_at<=? "
            "ORDER BY next_attempt_at LIMIT 1", (now,)).fetchone()
        if row:
            conn.execute("UPDATE leads SET status='processing', locked_at=?, updated_at=? WHERE id=?",
                         (now, now, row["id"]))
        conn.execute("COMMIT")
        return row
    except Exception:
        conn.execute("ROLLBACK")
        raise


def backoff_seconds(failures):
    base = BACKOFF_BASE_SECONDS * (2 ** (failures - 1))
    return base * random.uniform(0.9, 1.1)


def post_alert(cfg, text):
    """POST {"text": ...} to alert_webhook_url (Slack/Mattermost style). Returns (ok, error)."""
    url = cfg.get("alert_webhook_url")
    if not url:
        return False, "alert_webhook_url is not configured"
    try:
        r = requests.post(url, json={"text": text}, timeout=5)
        if r.status_code >= 300:
            return False, f"alert webhook answered HTTP {r.status_code}"
        return True, None
    except requests.RequestException as e:
        return False, redact(e, 300)


def send_alert(cfg, row, err):
    if not cfg.get("alert_webhook_url"):
        return
    text = (f":rotating_light: Lead {row['leadgen_id']} (page {row['page_id']}) is DEAD at stage "
            f"'{err.stage}' [{err.cls}]: {redact(err.message, 300)}")
    ok, why = post_alert(cfg, text)
    if not ok:
        log.warning("alert failed: %s", why)


def process_lead(row):
    cfg = get_config()
    lid = row["leadgen_id"]
    attempt_no = row["attempts"] + 1
    stage = "fetch"
    try:
        with closing(db()) as conn:
            if row["graph_data"]:
                graph = json.loads(row["graph_data"])
            elif row["payload"]:          # lead queued by an older version: resend as stored
                graph = None
            else:
                graph = fetch_lead(row, cfg)
                conn.execute("UPDATE leads SET graph_data=? WHERE id=?", (json.dumps(graph), row["id"]))
            stage = "map"
            if graph is not None:
                # rebuilt on every attempt, so a fixed mapping applies on requeue without refetching
                payload = build_payload(row, graph, cfg)
            else:
                payload = json.loads(row["payload"])
            missing = [f for f in (cfg.get("payload") or {}).get("required_fields", [])
                       if not str(payload.get(f, "")).strip()]
            if missing:
                raise LeadError("map", "permanent", f"Required field(s) empty after mapping: {', '.join(missing)}")
            conn.execute("UPDATE leads SET payload=? WHERE id=?", (json.dumps(payload), row["id"]))
            stage = "push"
            status, body = push_lead(lid, payload, cfg)
            now = now_iso()
            record_attempt(conn, lid, attempt_no, "push", True, http_status=status, body=body)
            clear = 1 if cfg.get("clear_payload_on_done") else 0
            conn.execute(
                "UPDATE leads SET status='done', attempts=?, done_at=?, updated_at=?, locked_at=NULL, "
                "last_error=NULL, last_error_class=NULL, last_error_stage=NULL, "
                "payload=CASE WHEN ? THEN NULL ELSE payload END, "
                "graph_data=CASE WHEN ? THEN NULL ELSE graph_data END WHERE id=?",
                (attempt_no, now, now, clear, clear, row["id"]))
        log.info("Lead %s done (created in ERPNext: %s, page=%s)", lid, erp_created_name(body),
                 cfg["pages"].get(str(row["page_id"]), {}).get("name", row["page_id"]))
    except LeadError as e:
        handle_failure(row, cfg, attempt_no, e)
    except Exception as e:  # unexpected: treat as transient, bounded by the retry limit
        log.exception("unexpected error on lead %s", lid)
        handle_failure(row, cfg, attempt_no, LeadError(stage, "transient", f"Unexpected error: {e!r}"))


def handle_failure(row, cfg, attempt_no, err):
    final = err.cls == "permanent" or attempt_no > MAX_RETRIES
    now = now_dt()
    with closing(db()) as conn:
        record_attempt(conn, row["leadgen_id"], attempt_no, err.stage, False, err.cls,
                       err.http_status, err.body, err.message)
        common = (attempt_no, redact(err.message, 1000), err.cls, err.stage, iso(now))
        if final:
            conn.execute(
                "UPDATE leads SET status='dead', attempts=?, last_error=?, last_error_class=?, "
                "last_error_stage=?, updated_at=?, dead_at=?, locked_at=NULL WHERE id=?",
                common + (iso(now), row["id"]))
            log.error("lead %s DEAD (%s/%s): %s", row["leadgen_id"], err.stage, err.cls, redact(err.message, 300))
        else:
            delay = backoff_seconds(attempt_no)
            conn.execute(
                "UPDATE leads SET status='pending', attempts=?, last_error=?, last_error_class=?, "
                "last_error_stage=?, updated_at=?, next_attempt_at=?, locked_at=NULL WHERE id=?",
                common + (iso(now + timedelta(seconds=delay)), row["id"]))
            log.warning("lead %s failed (%s/%s), retry in %ds: %s", row["leadgen_id"], err.stage,
                        err.cls, delay, redact(err.message, 300))
    if final:
        send_alert(cfg, row, err)


def heartbeat(worker_id):
    with closing(db()) as conn:
        conn.execute("INSERT INTO workers(worker_id,pid,last_seen) VALUES(?,?,?) "
                     "ON CONFLICT(worker_id) DO UPDATE SET last_seen=excluded.last_seen",
                     (worker_id, os.getpid(), now_iso()))


def maintenance():
    now = now_dt()
    with closing(db()) as conn:
        stale = iso(now - timedelta(seconds=STALE_PROCESSING_SECONDS))
        n = conn.execute("UPDATE leads SET status='pending', locked_at=NULL, updated_at=? "
                         "WHERE status='processing' AND locked_at<?", (iso(now), stale)).rowcount
        if n:
            log.warning("reset %d stale processing lead(s)", n)
        done_cut = iso(now - timedelta(days=DONE_RETENTION_DAYS))
        dead_cut = iso(now - timedelta(days=DEAD_RETENTION_DAYS))
        old = ("SELECT leadgen_id FROM leads WHERE (status='done' AND done_at<?) "
               "OR (status='dead' AND dead_at<?)")
        conn.execute(f"DELETE FROM lead_attempts WHERE leadgen_id IN ({old})", (done_cut, dead_cut))
        conn.execute("DELETE FROM leads WHERE (status='done' AND done_at<?) OR (status='dead' AND dead_at<?)",
                     (done_cut, dead_cut))
        conn.execute("DELETE FROM workers WHERE last_seen<?", (iso(now - timedelta(hours=1)),))


def run_once():
    """Claim and process up to BATCH_SIZE due leads. Returns how many were handled."""
    handled = 0
    with closing(db()) as conn:
        while handled < BATCH_SIZE:
            row = claim_one(conn)
            if not row:
                break
            process_lead(row)
            handled += 1
    return handled


def worker_loop(worker_id):
    log.info("Worker thread started. (%s)", worker_id)
    last_maint = 0.0
    while True:
        try:
            heartbeat(worker_id)
            if time.time() - last_maint > MAINTENANCE_EVERY_SECONDS:
                maintenance()
                last_maint = time.time()
            if run_once() == 0:
                time.sleep(WORKER_IDLE_SECONDS)
        except Exception:
            log.exception("worker loop error")
            time.sleep(WORKER_IDLE_SECONDS)


_workers_started = False


def start_workers():
    global _workers_started
    if _workers_started:
        return
    _workers_started = True
    try:
        n = int(get_config().get("worker_threads", 1))
    except Exception:
        n = 1
    n = max(1, min(8, n))
    host = socket.gethostname()
    for i in range(n):
        wid = f"{host}-{os.getpid()}-{i}"
        threading.Thread(target=worker_loop, args=(wid,), name=f"worker-{i}", daemon=True).start()


# ---------------------------------------------------------------- webhook routes
def valid_signature(raw, header, secret):
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected.encode(), header[len("sha256="):].encode())


@app.get("/webhook/facebook-leads")
def verify_webhook():
    cfg = get_config()
    token = request.args.get("hub.verify_token", "")
    if request.args.get("hub.mode") == "subscribe" and hmac.compare_digest(token.encode(), cfg["verify_token"].encode()):
        return request.args.get("hub.challenge", ""), 200
    return "Forbidden", 403


@app.post("/webhook/facebook-leads")
def receive_webhook():
    cfg = get_config()
    raw = request.get_data()
    if not valid_signature(raw, request.headers.get("X-Hub-Signature-256"), cfg["app_secret"]):
        log.warning("webhook rejected: bad signature")
        return "Forbidden", 403
    try:
        body = json.loads(raw)
    except ValueError:
        return "Bad JSON", 400

    try:
        return _queue_leads(body, cfg)
    except sqlite3.Error:
        log.exception("webhook: database error")
        return jsonify({"status": "error"}), 500


def _queue_leads(body, cfg):
    now = now_iso()
    queued = duplicates = skipped = 0
    with closing(db()) as conn:
        for entry in body.get("entry", []) or []:
            for change in entry.get("changes", []) or []:
                if change.get("field") != "leadgen":
                    continue
                v = change.get("value") or {}
                lid = v.get("leadgen_id")
                if not lid:
                    continue
                page_id = str(v.get("page_id") or entry.get("id") or "")
                if page_id not in cfg["pages"]:
                    log.warning("Webhook: lead %s skipped, page %s is not configured", lid, page_id)
                    skipped += 1
                    continue
                cur = conn.execute(
                    "INSERT OR IGNORE INTO leads(leadgen_id,page_id,form_id,status,attempts,next_attempt_at,"
                    "webhook_data,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (str(lid), page_id, str(v.get("form_id") or ""), "pending", 0, now,
                     json.dumps(v), now, now))
                if cur.rowcount:
                    queued += 1
                else:
                    duplicates += 1
    log.info("Webhook: %d queued, %d duplicate, %d skipped", queued, duplicates, skipped)
    return jsonify({"status": "ok", "queued": queued, "duplicates": duplicates, "skipped": skipped}), 200


@app.get("/health")
def health():
    try:
        with closing(db()) as conn:
            conn.execute("SELECT 1").fetchone()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": redact(e)}), 503


# ---------------------------------------------------------------- admin routes
def admin_required(fn):
    @wraps(fn)
    def wrapper(*a, **kw):
        key = request.headers.get("X-Admin-Key", "")
        if not hmac.compare_digest(key.encode(), get_config()["admin_api_key"].encode()):
            return jsonify({"error": "unauthorized"}), 401
        return fn(*a, **kw)
    return wrapper


@app.get("/admin/stats")
@admin_required
def admin_stats():
    now = now_dt()
    with closing(db()) as conn:
        counts = {s: 0 for s in ("pending", "processing", "done", "dead")}
        for r in conn.execute("SELECT status, COUNT(*) c FROM leads GROUP BY status"):
            counts[r["status"]] = r["c"]
        oldest = conn.execute("SELECT MIN(created_at) m FROM leads WHERE status IN ('pending','processing')").fetchone()["m"]
        age = int((now - parse_iso(oldest)).total_seconds()) if oldest else 0
        pages = {}
        for r in conn.execute("SELECT page_id, MAX(done_at) last_success FROM leads WHERE status='done' GROUP BY page_id"):
            pages[r["page_id"]] = {"last_success": r["last_success"], "failures_last_hour": 0}
        hour_ago = iso(now - timedelta(hours=1))
        for r in conn.execute(
                "SELECT l.page_id, COUNT(*) c FROM lead_attempts a JOIN leads l ON l.leadgen_id=a.leadgen_id "
                "WHERE a.ok=0 AND a.created_at>=? GROUP BY l.page_id", (hour_ago,)):
            pages.setdefault(r["page_id"], {"last_success": None, "failures_last_hour": 0})["failures_last_hour"] = r["c"]
        alive = conn.execute("SELECT COUNT(*) c FROM workers WHERE last_seen>=?",
                             (iso(now - timedelta(seconds=HEARTBEAT_ALIVE_SECONDS)),)).fetchone()["c"]
    for pid in get_config()["pages"]:
        pages.setdefault(str(pid), {"last_success": None, "failures_last_hour": 0})
    return jsonify({"counts": counts, "oldest_unfinished_age_seconds": age,
                    "pages": pages, "workers_alive": alive})


@app.post("/admin/test-alert")
@admin_required
def admin_test_alert():
    ok, why = post_alert(get_config(), ":white_check_mark: Test alert from the Facebook leads webhook.")
    return jsonify({"sent": ok, "error": why}), (200 if ok else 400)


@app.get("/admin/dead-leads")
@admin_required
def admin_dead_leads():
    where, args = ["status='dead'"], []
    if request.args.get("page_id"):
        where.append("page_id=?"); args.append(request.args["page_id"])
    if request.args.get("since"):
        where.append("dead_at>=?"); args.append(request.args["since"])
    if request.args.get("until"):
        where.append("dead_at<?"); args.append(request.args["until"])
    try:
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        return jsonify({"error": "limit/offset must be integers"}), 400
    w = " AND ".join(where)
    with closing(db()) as conn:
        total = conn.execute(f"SELECT COUNT(*) c FROM leads WHERE {w}", args).fetchone()["c"]
        rows = conn.execute(
            f"SELECT leadgen_id,page_id,form_id,attempts,requeues,last_error,last_error_class,"
            f"last_error_stage,created_at,dead_at FROM leads WHERE {w} ORDER BY dead_at DESC LIMIT ? OFFSET ?",
            args + [limit, offset]).fetchall()
    return jsonify({"total": total, "limit": limit, "offset": offset, "leads": [dict(r) for r in rows]})


@app.get("/admin/dead-leads/<leadgen_id>")
@admin_required
def admin_dead_lead(leadgen_id):
    with closing(db()) as conn:
        row = conn.execute("SELECT * FROM leads WHERE leadgen_id=? AND status='dead'", (leadgen_id,)).fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        attempts = conn.execute("SELECT * FROM lead_attempts WHERE leadgen_id=? ORDER BY id", (leadgen_id,)).fetchall()
    lead = dict(row)
    for k in ("payload", "graph_data", "webhook_data"):
        lead[k] = json.loads(lead[k]) if lead[k] else None
    return jsonify({"lead": lead, "attempts": [dict(a) for a in attempts]})


@app.post("/admin/requeue")
@admin_required
def admin_requeue():
    body = request.get_json(silent=True) or {}
    where, args = ["status='dead'"], []
    ids = body.get("leadgen_ids")
    if ids:
        if not isinstance(ids, list):
            return jsonify({"error": "leadgen_ids must be a list"}), 400
        where.append(f"leadgen_id IN ({','.join('?' * len(ids))})"); args += [str(i) for i in ids]
    elif body.get("page_id") or body.get("since") or body.get("until"):
        if body.get("page_id"):
            where.append("page_id=?"); args.append(str(body["page_id"]))
        if body.get("since"):
            where.append("dead_at>=?"); args.append(body["since"])
        if body.get("until"):
            where.append("dead_at<?"); args.append(body["until"])
    elif body.get("all") is not True:
        return jsonify({"error": "give leadgen_ids, page_id/since/until, or all=true"}), 400

    max_requeues = int(get_config().get("max_requeues", 5))
    refetch = bool(body.get("refetch"))
    requeued, skipped = [], []
    now = now_iso()
    with closing(db()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            rows = conn.execute(f"SELECT id, leadgen_id, requeues FROM leads WHERE {' AND '.join(where)}", args).fetchall()
            for r in rows:
                if r["requeues"] >= max_requeues:
                    skipped.append(r["leadgen_id"]); continue
                conn.execute(
                    "UPDATE leads SET status='pending', attempts=0, requeues=requeues+1, next_attempt_at=?, "
                    "updated_at=?, dead_at=NULL, locked_at=NULL, payload=CASE WHEN ? THEN NULL ELSE payload END, "
                    "graph_data=CASE WHEN ? THEN NULL ELSE graph_data END WHERE id=?",
                    (now, now, 1 if refetch else 0, 1 if refetch else 0, r["id"]))
                requeued.append(r["leadgen_id"])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK"); raise
    return jsonify({"requeued": len(requeued), "skipped_over_max_requeues": skipped, "leadgen_ids": requeued})


# ---------------------------------------------------------------- startup
try:
    get_config()
except RuntimeError as _e:
    log.critical("Invalid configuration: %s", _e)
    raise
init_db()
log.info("Database ready at %s", DATABASE_PATH)
if os.environ.get("DISABLE_WORKER") != "1":
    start_workers()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)