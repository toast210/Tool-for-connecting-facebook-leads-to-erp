import hashlib
import hmac
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("fb-leads-erpnext")

app = Flask(__name__)

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------

CREDENTIALS_PATH = os.environ.get("CREDENTIALS_PATH", "credentials.json")
DATABASE_PATH = os.environ.get("DATABASE_PATH", "leads.db")

with open(CREDENTIALS_PATH, "r") as f:
    CREDS = json.load(f)


def _require(key: str):
    if key not in CREDS or not CREDS[key]:
        raise RuntimeError(f"credentials.json is missing required key: {key}")
    return CREDS[key]


VERIFY_TOKEN = _require("verify_token")
APP_SECRET = _require("app_secret")
ADMIN_API_KEY = _require("admin_api_key")
GRAPH_VERSION = CREDS.get("graph_api_version", "v26.0")

ERP_BASE_URL = CREDS["erpnext"]["base_url"].rstrip("/")
ERP_API_KEY = CREDS["erpnext"]["api_key"]
ERP_API_SECRET = CREDS["erpnext"]["api_secret"]
# Name of the custom field on the ERPNext Lead doctype that stores the Facebook lead ID.
ERP_ID_FIELD = CREDS["erpnext"].get("leadgen_id_field", "leadgen_id")
# Ask ERPNext whether the lead already exists before inserting (prevents duplicates on retry).
ERP_DUPLICATE_CHECK = CREDS["erpnext"].get("duplicate_check", True)

PAGES = CREDS["pages"]

# Optional: a webhook URL that receives {"text": "..."} when a lead dies (Slack/Mattermost style).
ALERT_WEBHOOK_URL = CREDS.get("alert_webhook_url")

# Fields requested from the Graph API Lead object. Everything returned (besides the special
# handling of field_data / custom_disclaimer_responses) is copied into the payload as-is.
# Ad-level fields (ad_*, adset_*, campaign_*) need the ads_management permission on the token.
# Override with "graph_lead_fields" in credentials.json if your API version rejects a field.
DEFAULT_GRAPH_FIELDS = [
    "id", "created_time", "ad_id", "ad_name", "adset_id", "adset_name",
    "campaign_id", "campaign_name", "form_id", "is_organic", "platform",
    "partner_name", "custom_disclaimer_responses", "field_data",
]
_gf = CREDS.get("graph_lead_fields", DEFAULT_GRAPH_FIELDS)
if isinstance(_gf, str):
    _gf = [x.strip() for x in _gf.split(",") if x.strip()]
GRAPH_LEAD_FIELDS = ",".join(_gf)
# Keys of the Graph response that are handled specially (not copied verbatim).
GRAPH_SPECIAL_KEYS = {"id", "field_data", "custom_disclaimer_responses"}

# Retry policy: attempt 1 fails -> wait 1 min, then 2, 4, 8, 16 min; the 6th failure is final.
MAX_RETRIES = 5
MAX_ATTEMPTS = MAX_RETRIES + 1
BACKOFF_BASE_SECONDS = 60
BACKOFF_JITTER = 0.10  # +/-10% so leads that failed together do not retry in lockstep

WORKER_POLL_SECONDS = 5
BATCH_SIZE = 20  # max leads processed per loop, then a short pause (protects ERPNext after outages)
STALE_PROCESSING_SECONDS = 300
HTTP_TIMEOUT = 15

ERROR_TEXT_LIMIT = 2000  # stored response bodies
LAST_ERROR_LIMIT = 500  # short text kept on the lead row
DONE_RETENTION_DAYS = 30
DEAD_RETENTION_DAYS = 90  # Facebook stops returning leads after ~90 days anyway
MAX_REQUEUES = int(CREDS.get("max_requeues", 5))

# The exact payload sent to ERPNext is stored on the lead row (so retries skip the Graph fetch and
# you can see what was sent). Set "clear_payload_on_done": true to erase it once the lead succeeds;
# otherwise it is kept until the row is purged (DONE_RETENTION_DAYS / DEAD_RETENTION_DAYS).
CLEAR_PAYLOAD_ON_DONE = bool(CREDS.get("clear_payload_on_done", False))

# Worker threads (1 is enough for most setups; claiming is atomic so more is safe).
WORKER_THREADS = max(1, min(8, int(CREDS.get("worker_threads", 1))))

TRANSIENT = "transient"
TOKEN = "token"
PERMANENT = "permanent"

# Graph API error codes
GRAPH_TOKEN_CODES = {10, 102, 190}  # permission / session / OAuth problems (+ 200-299 below)
GRAPH_TRANSIENT_CODES = {1, 2, 4, 17, 32, 341, 613}  # unknown, throttling, rate limits

# Values that must never end up in logs or the database.
SECRETS = [ERP_API_SECRET, f"{ERP_API_KEY}:{ERP_API_SECRET}", APP_SECRET, ADMIN_API_KEY]
SECRETS += [p.get("page_access_token") for p in PAGES.values()]

STOP = threading.Event()

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return iso(utcnow())


def parse_iso(value):
    """Parse an ISO 8601 string into our canonical UTC format. Returns None for empty input."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"Invalid datetime: {value!r} (use ISO 8601, e.g. 2026-10-05T08:00:00Z)")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return iso(dt.astimezone(timezone.utc))


def sanitize(text, limit=ERROR_TEXT_LIMIT):
    """Strip tokens/secrets from text and truncate. Use before logging or storing any error."""
    if text is None:
        return None
    text = str(text)
    text = re.sub(r"(access_token=)[^&\s'\"]+", r"\1REDACTED", text)
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "REDACTED")
    return text[:limit]


# --------------------------------------------------------------------------------------
# Database (SQLite, WAL mode, one connection per thread)
# --------------------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    leadgen_id        TEXT PRIMARY KEY,
    page_id           TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','processing','done','dead')),
    attempts          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   TEXT NOT NULL,
    last_error        TEXT,
    last_error_stage  TEXT,
    last_error_class  TEXT,
    last_error_at     TEXT,
    requeue_count     INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    died_at           TEXT,
    completed_at      TEXT,
    payload           TEXT,                     -- JSON exactly as sent to ERPNext
    webhook_value     TEXT                      -- JSON of the webhook's change.value (fallback data)
);

CREATE INDEX IF NOT EXISTS idx_leads_due ON leads (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS lead_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id      TEXT NOT NULL REFERENCES leads(leadgen_id) ON DELETE CASCADE,
    attempt_no      INTEGER NOT NULL,
    stage           TEXT NOT NULL,
    classification  TEXT,
    http_status     INTEGER,
    exception_class TEXT,
    response_body   TEXT,
    duration_ms     INTEGER,
    success         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempts_lead ON lead_attempts (leadgen_id, attempt_no);
"""

_local = threading.local()


def db() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        # isolation_level=None -> autocommit; multi-statement work uses explicit BEGIN/COMMIT.
        conn = sqlite3.connect(DATABASE_PATH, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


def init_db():
    conn = db()
    conn.executescript(SCHEMA)
    # Migration for databases created before the payload column existed.
    columns = [r["name"] for r in conn.execute("PRAGMA table_info(leads)")]
    for column in ("payload", "webhook_value"):
        if column not in columns:
            conn.execute(f"ALTER TABLE leads ADD COLUMN {column} TEXT")
            log.info("Added '%s' column to leads table.", column)
    log.info("Database ready at %s", DATABASE_PATH)


# --------------------------------------------------------------------------------------
# Errors and HTTP helpers
# --------------------------------------------------------------------------------------


class StageError(Exception):
    """A failure in one stage ('fetch' = Graph API, 'push' = ERPNext)."""

    def __init__(self, stage, classification, message, http_status=None,
                 exc_class=None, body=None, duration_ms=None):
        super().__init__(message)
        self.stage = stage
        self.classification = classification
        self.message = message
        self.http_status = http_status
        self.exc_class = exc_class
        self.body = body
        self.duration_ms = duration_ms


def classify_graph(resp) -> str:
    code = None
    try:
        code = (resp.json().get("error") or {}).get("code")
    except (ValueError, AttributeError):
        pass
    if code in GRAPH_TOKEN_CODES or (isinstance(code, int) and 200 <= code <= 299):
        return TOKEN
    if resp.status_code == 429 or resp.status_code >= 500 or code in GRAPH_TRANSIENT_CODES:
        return TRANSIENT
    return PERMANENT


def classify_erp(resp) -> str:
    status = resp.status_code
    if status >= 500 or status in (408, 429):
        return TRANSIENT
    if status in (401, 403):
        return TOKEN  # ERPNext API credentials/permissions: affects every lead, fix then requeue
    return PERMANENT  # e.g. 417 validation / mandatory field errors


def http_call(stage, method, url, classify, **kwargs):
    """Make one HTTP call. Returns (response, duration_ms) or raises StageError."""
    start = time.monotonic()
    try:
        resp = requests.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
    except requests.RequestException as e:
        raise StageError(
            stage, TRANSIENT, f"{type(e).__name__}: {e}",
            exc_class=type(e).__name__,
            duration_ms=int((time.monotonic() - start) * 1000),
        )
    ms = int((time.monotonic() - start) * 1000)
    if resp.status_code >= 400:
        raise StageError(
            stage, classify(resp), f"HTTP {resp.status_code}",
            http_status=resp.status_code, exc_class="HTTPError",
            body=resp.text, duration_ms=ms,
        )
    return resp, ms


def json_or_error(stage, resp):
    try:
        return resp.json()
    except ValueError:
        raise StageError(stage, TRANSIENT, "Response was not valid JSON",
                         http_status=resp.status_code, exc_class="JSONDecodeError", body=resp.text)


# --------------------------------------------------------------------------------------
# Facebook + ERPNext calls
# --------------------------------------------------------------------------------------


def fetch_lead_from_facebook(leadgen_id: str, page_config: dict) -> dict:
    """Fetch the lead from the Graph API and return a flat dict of every field it gives us:
    all form answers plus ad/campaign/form metadata and the creation time."""
    resp, _ = http_call(
        "fetch", "GET", f"https://graph.facebook.com/{GRAPH_VERSION}/{leadgen_id}", classify_graph,
        params={"access_token": page_config["page_access_token"], "fields": GRAPH_LEAD_FIELDS},
    )
    data = json_or_error("fetch", resp)

    fields = {}

    # Form answers. A question can have several values (multi-select): keep them all.
    for f in data.get("field_data") or []:
        values = [str(v) for v in (f.get("values") or [])]
        fields[f["name"]] = ", ".join(values)

    # Answers to optional disclaimer checkboxes (not included in field_data).
    disclaimers = data.get("custom_disclaimer_responses") or []
    if disclaimers:
        if all(isinstance(d, dict) and d.get("checkbox_key") and "is_checked" in d for d in disclaimers):
            for d in disclaimers:
                fields[f"disclaimer_{d['checkbox_key']}"] = d["is_checked"]
        else:
            fields["custom_disclaimer_responses"] = json.dumps(disclaimers, ensure_ascii=False)

    # Everything else the Graph API returned (campaign_name, adset_name, ad_name, ad_id, form_id,
    # created_time, platform, is_organic, ...). Nested values are stored as JSON text.
    for key, value in data.items():
        if key in GRAPH_SPECIAL_KEYS or value is None:
            continue
        fields[key] = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value

    return fields


def erp_headers() -> dict:
    return {
        "Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}",
        "Content-Type": "application/json",
    }


def erp_find_existing(leadgen_id: str):
    """Return the name of an existing ERPNext Lead with this leadgen_id, or None."""
    params = {
        "filters": json.dumps([[ERP_ID_FIELD, "=", leadgen_id]]),
        "fields": json.dumps(["name"]),
        "limit_page_length": 1,
    }
    resp, _ = http_call(
        "push", "GET", f"{ERP_BASE_URL}/api/resource/Lead", classify_erp,
        params=params, headers=erp_headers(),
    )
    data = json_or_error("push", resp).get("data") or []
    return data[0]["name"] if data else None


def build_payload(leadgen_id: str, fields: dict, page_config: dict, page_id: str,
                  webhook_value: dict = None) -> dict:
    """The exact JSON sent to ERPNext: every Facebook field as-is, plus lead_source, page_id and
    the lead ID. Values the webhook itself carried (ad_id, form_id, created_time) are used only
    as a fallback when the Graph API did not return them (e.g. missing ads_management)."""
    payload = dict(fields)

    wv = webhook_value or {}
    created = wv.get("created_time")
    if isinstance(created, (int, float)):
        created = datetime.fromtimestamp(created, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
    fallback = {
        "ad_id": wv.get("ad_id") or wv.get("adgroup_id"),
        "form_id": wv.get("form_id"),
        "created_time": created,
    }
    for key, value in fallback.items():
        if value not in (None, "") and key not in payload:
            payload[key] = value

    payload["page_id"] = page_id
    payload["lead_source"] = page_config.get("source", "Facebook Lead Ads")
    payload[ERP_ID_FIELD] = leadgen_id
    return payload


def push_lead_to_erpnext(payload: dict):
    """Send the payload to ERPNext as-is (mapping is done on the ERPNext side).

    Returns (created_name, already_existed).
    """
    try:
        resp, _ = http_call(
            "push", "POST", f"{ERP_BASE_URL}/api/resource/Lead", classify_erp,
            json=payload, headers=erp_headers(),
        )
    except StageError as e:
        # A unique-constraint rejection on our own ID field means an earlier attempt succeeded.
        if e.http_status == 409 and ERP_ID_FIELD in (e.body or ""):
            return None, True
        raise
    return json_or_error("push", resp)["data"]["name"], False


# --------------------------------------------------------------------------------------
# Queue operations
# --------------------------------------------------------------------------------------


def enqueue_lead(leadgen_id: str, page_id: str, webhook_value: dict = None) -> bool:
    """Insert a lead into the queue. Returns True if new, False if it was already queued."""
    now = now_iso()
    cur = db().execute(
        "INSERT OR IGNORE INTO leads (leadgen_id, page_id, status, attempts, next_attempt_at,"
        " created_at, updated_at, webhook_value) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)",
        (leadgen_id, page_id, now, now, now,
         json.dumps(webhook_value, ensure_ascii=False) if webhook_value else None),
    )
    return cur.rowcount == 1


def claim_one():
    """Atomically claim the oldest due lead (pending -> processing, attempts + 1)."""
    conn = db()
    now = now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT leadgen_id FROM leads WHERE status='pending' AND next_attempt_at <= ?"
            " ORDER BY next_attempt_at LIMIT 1",
            (now,),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None
        conn.execute(
            "UPDATE leads SET status='processing', attempts=attempts+1, updated_at=?"
            " WHERE leadgen_id=?",
            (now, row["leadgen_id"]),
        )
        lead = conn.execute(
            "SELECT * FROM leads WHERE leadgen_id=?", (row["leadgen_id"],)
        ).fetchone()
        conn.execute("COMMIT")
        return lead
    except Exception:
        conn.execute("ROLLBACK")
        raise


def reset_stale_processing():
    """Recover leads left in 'processing' by a crash."""
    cutoff = iso(utcnow() - timedelta(seconds=STALE_PROCESSING_SECONDS))
    cur = db().execute(
        "UPDATE leads SET status='pending', updated_at=? WHERE status='processing' AND updated_at < ?",
        (now_iso(), cutoff),
    )
    if cur.rowcount:
        log.warning("Reset %d stale 'processing' lead(s) to pending.", cur.rowcount)


def purge_old():
    done_cutoff = iso(utcnow() - timedelta(days=DONE_RETENTION_DAYS))
    dead_cutoff = iso(utcnow() - timedelta(days=DEAD_RETENTION_DAYS))
    cur = db().execute(
        "DELETE FROM leads WHERE (status='done' AND completed_at < ?)"
        " OR (status='dead' AND died_at < ?)",
        (done_cutoff, dead_cutoff),
    )
    if cur.rowcount:
        log.info("Purged %d old lead(s).", cur.rowcount)


def record_attempt(leadgen_id, attempt_no, stage, success, classification=None, http_status=None,
                   exc_class=None, body=None, duration_ms=None):
    db().execute(
        "INSERT INTO lead_attempts (leadgen_id, attempt_no, stage, classification, http_status,"
        " exception_class, response_body, duration_ms, success, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (leadgen_id, attempt_no, stage, classification, http_status, exc_class,
         sanitize(body), duration_ms, 1 if success else 0, now_iso()),
    )


def mark_done(leadgen_id: str):
    now = now_iso()
    clear = ", payload=NULL" if CLEAR_PAYLOAD_ON_DONE else ""
    db().execute(
        f"UPDATE leads SET status='done', completed_at=?, updated_at=?{clear} WHERE leadgen_id=?",
        (now, now, leadgen_id),
    )


def notify_dead(leadgen_id: str, page_id: str, err: StageError):
    page_name = PAGES.get(page_id, {}).get("name", page_id)
    msg = (f"Lead {leadgen_id} (page {page_name}) is DEAD. "
           f"Stage={err.stage} class={err.classification} error={sanitize(err.message, 300)}")
    log.error(msg)
    if not ALERT_WEBHOOK_URL:
        return
    try:
        requests.post(ALERT_WEBHOOK_URL, json={"text": msg}, timeout=10)
    except Exception:
        log.exception("Could not send dead-lead alert.")


def handle_failure(lead, err: StageError):
    leadgen_id = lead["leadgen_id"]
    attempts = lead["attempts"]
    now = utcnow()

    record_attempt(
        leadgen_id, attempts, err.stage, False, classification=err.classification,
        http_status=err.http_status, exc_class=err.exc_class,
        body=err.body if err.body else err.message, duration_ms=err.duration_ms,
    )

    short_error = sanitize(
        f"{err.message}: {err.body}" if err.body else err.message, LAST_ERROR_LIMIT
    )

    dead = err.classification == PERMANENT or attempts >= MAX_ATTEMPTS
    if dead:
        db().execute(
            "UPDATE leads SET status='dead', died_at=?, last_error=?, last_error_stage=?,"
            " last_error_class=?, last_error_at=?, updated_at=? WHERE leadgen_id=?",
            (iso(now), short_error, err.stage, err.classification, iso(now), iso(now), leadgen_id),
        )
        notify_dead(leadgen_id, lead["page_id"], err)
        return

    delay = BACKOFF_BASE_SECONDS * (2 ** (attempts - 1))
    delay *= 1 + random.uniform(-BACKOFF_JITTER, BACKOFF_JITTER)
    next_at = now + timedelta(seconds=delay)
    db().execute(
        "UPDATE leads SET status='pending', next_attempt_at=?, last_error=?, last_error_stage=?,"
        " last_error_class=?, last_error_at=?, updated_at=? WHERE leadgen_id=?",
        (iso(next_at), short_error, err.stage, err.classification, iso(now), iso(now), leadgen_id),
    )
    log.warning(
        "Lead %s failed (attempt %d/%d, stage=%s, class=%s). Retrying in %ds.",
        leadgen_id, attempts, MAX_ATTEMPTS, err.stage, err.classification, int(delay),
    )


def process_lead(lead):
    leadgen_id = lead["leadgen_id"]
    attempt_no = lead["attempts"]
    page_config = PAGES.get(lead["page_id"])
    started = time.monotonic()
    stage = "fetch"

    try:
        if page_config is None:
            raise StageError("fetch", PERMANENT, f"Page ID {lead['page_id']} not in configuration")

        payload = None
        if lead["payload"]:
            try:
                payload = json.loads(lead["payload"])  # stored on an earlier attempt: skip Graph
            except ValueError:
                log.warning("Stored payload for %s is unreadable; refetching.", leadgen_id)
        if payload is None:
            fields = fetch_lead_from_facebook(leadgen_id, page_config)
            try:
                webhook_value = json.loads(lead["webhook_value"]) if lead["webhook_value"] else None
            except ValueError:
                webhook_value = None
            payload = build_payload(leadgen_id, fields, page_config, lead["page_id"], webhook_value)
            db().execute(
                "UPDATE leads SET payload=?, updated_at=? WHERE leadgen_id=?",
                (json.dumps(payload, ensure_ascii=False), now_iso(), leadgen_id),
            )

        stage = "push"
        existing = erp_find_existing(leadgen_id) if ERP_DUPLICATE_CHECK else None
        if existing:
            created, note = existing, "already existed in ERPNext"
        else:
            created, already = push_lead_to_erpnext(payload)
            note = "already existed in ERPNext" if already else "created in ERPNext"
            created = created or "(existing)"

    except StageError as err:
        handle_failure(lead, err)
        return
    except Exception as e:  # a bug or something unexpected: treat as transient, keep the lead safe
        log.exception("Unexpected error while processing lead %s", leadgen_id)
        handle_failure(lead, StageError(stage, TRANSIENT, f"Unexpected {type(e).__name__}: {e}",
                                        exc_class=type(e).__name__))
        return

    record_attempt(
        leadgen_id, attempt_no, "push", True, body=f"{note}: {created}",
        duration_ms=int((time.monotonic() - started) * 1000),
    )
    mark_done(leadgen_id)
    log.info("Lead %s done (%s: %s, page=%s).", leadgen_id, note, created,
             page_config.get("name"))


# --------------------------------------------------------------------------------------
# Worker thread
# --------------------------------------------------------------------------------------


def worker_loop():
    log.info("Worker thread started.")
    last_purge = 0.0
    while not STOP.is_set():
        wait = WORKER_POLL_SECONDS
        try:
            reset_stale_processing()
            if time.time() - last_purge > 3600:
                purge_old()
                last_purge = time.time()

            processed = 0
            while processed < BATCH_SIZE and not STOP.is_set():
                lead = claim_one()
                if lead is None:
                    break
                process_lead(lead)
                processed += 1
            if processed:
                wait = 1  # more may be waiting; short pause keeps ERPNext from being hammered
        except Exception:
            log.exception("Worker loop error (will keep running).")
        STOP.wait(wait)
    log.info("Worker thread stopped.")


_worker_threads = []


def start_worker():
    if any(t.is_alive() for t in _worker_threads):
        return
    _worker_threads.clear()
    for i in range(WORKER_THREADS):
        t = threading.Thread(target=worker_loop, name=f"lead-worker-{i + 1}", daemon=True)
        t.start()
        _worker_threads.append(t)


# --------------------------------------------------------------------------------------
# Webhook
# --------------------------------------------------------------------------------------


def valid_signature(raw_body: bytes, header: str) -> bool:
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(APP_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header[len("sha256="):])


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
    raw = request.get_data()
    if not valid_signature(raw, request.headers.get("X-Hub-Signature-256", "")):
        log.warning("Webhook rejected: invalid signature.")
        return "Forbidden", 403

    try:
        payload = json.loads(raw or b"{}")
    except ValueError:
        return jsonify({"error": "invalid JSON"}), 400

    queued = duplicates = skipped = 0
    try:
        for entry in payload.get("entry", []):
            page_id = str(entry.get("id"))
            if page_id not in PAGES:
                log.warning("Lead from unrecognized Page ID %s -- skipping.", page_id)
                skipped += 1
                continue
            for change in entry.get("changes", []):
                value = change.get("value") or {}
                leadgen_id = value.get("leadgen_id")
                if not leadgen_id:
                    continue
                if enqueue_lead(str(leadgen_id), page_id, value):
                    queued += 1
                else:
                    duplicates += 1
    except sqlite3.Error:
        # Storage problem: answer 5xx so Facebook resends the webhook later.
        log.exception("Could not store lead(s) -- asking Facebook to retry.")
        return jsonify({"error": "storage unavailable"}), 503

    log.info("Webhook: %d queued, %d duplicate, %d skipped.", queued, duplicates, skipped)
    return jsonify({"status": "received", "queued": queued, "duplicates": duplicates,
                    "skipped": skipped}), 200


# --------------------------------------------------------------------------------------
# Admin API (for ERPNext)
# --------------------------------------------------------------------------------------


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        supplied = request.headers.get("X-Admin-Key", "")
        if not hmac.compare_digest(supplied.encode(), ADMIN_API_KEY.encode()):
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


def _int_arg(name, default, minimum, maximum):
    raw = request.args.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer")
    return max(minimum, min(maximum, value))


def _lead_summary(row) -> dict:
    return {
        "leadgen_id": row["leadgen_id"],
        "page_id": row["page_id"],
        "page_name": PAGES.get(row["page_id"], {}).get("name"),
        "attempts": row["attempts"],
        "last_error_stage": row["last_error_stage"],
        "last_error_class": row["last_error_class"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "died_at": row["died_at"],
        "requeue_count": row["requeue_count"],
    }


@app.route("/admin/dead-leads", methods=["GET"])
@admin_required
def admin_dead_leads():
    try:
        limit = _int_arg("limit", 50, 1, 200)
        offset = _int_arg("offset", 0, 0, 10_000_000)
        since = parse_iso(request.args.get("since"))
        until = parse_iso(request.args.get("until"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    where, params = ["status='dead'"], []
    if request.args.get("page_id"):
        where.append("page_id=?")
        params.append(request.args["page_id"])
    if since:
        where.append("died_at >= ?")
        params.append(since)
    if until:
        where.append("died_at <= ?")
        params.append(until)
    clause = " AND ".join(where)

    conn = db()
    total = conn.execute(f"SELECT COUNT(*) FROM leads WHERE {clause}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM leads WHERE {clause} ORDER BY died_at DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    return jsonify({
        "count": len(rows), "total": total, "limit": limit, "offset": offset,
        "leads": [_lead_summary(r) for r in rows],
    })


@app.route("/admin/dead-leads/<leadgen_id>", methods=["GET"])
@admin_required
def admin_dead_lead_detail(leadgen_id):
    conn = db()
    row = conn.execute(
        "SELECT * FROM leads WHERE leadgen_id=? AND status='dead'", (leadgen_id,)
    ).fetchone()
    if row is None:
        return jsonify({"error": "not found (only dead leads are exposed)"}), 404

    attempts = conn.execute(
        "SELECT attempt_no, stage, classification, http_status, exception_class, response_body,"
        " duration_ms, success, created_at FROM lead_attempts WHERE leadgen_id=?"
        " ORDER BY id",
        (leadgen_id,),
    ).fetchall()

    result = _lead_summary(row)
    result["status"] = row["status"]
    # Exactly what was (or would be) sent to ERPNext. Null if the Graph fetch never succeeded.
    result["payload"] = json.loads(row["payload"]) if row["payload"] else None
    result["attempts_history"] = [dict(a) for a in attempts]
    return jsonify(result)


@app.route("/admin/requeue", methods=["POST"])
@admin_required
def admin_requeue():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON body required"}), 400

    where, params = ["status='dead'"], []
    has_filter = body.get("all") is True

    ids = body.get("leadgen_ids")
    if ids is not None:
        if not isinstance(ids, list) or not ids or len(ids) > 500 \
                or not all(isinstance(i, str) for i in ids):
            return jsonify({"error": "leadgen_ids must be a list of 1-500 strings"}), 400
        where.append(f"leadgen_id IN ({','.join('?' * len(ids))})")
        params.extend(ids)
        has_filter = True

    if body.get("page_id"):
        where.append("page_id=?")
        params.append(str(body["page_id"]))
        has_filter = True

    try:
        since, until = parse_iso(body.get("since")), parse_iso(body.get("until"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if since:
        where.append("died_at >= ?")
        params.append(since)
        has_filter = True
    if until:
        where.append("died_at <= ?")
        params.append(until)
        has_filter = True

    if not has_filter:
        return jsonify({"error": "provide leadgen_ids, page_id, since/until, or all=true"}), 400

    clause = " AND ".join(where)
    conn = db()
    skipped = conn.execute(
        f"SELECT COUNT(*) FROM leads WHERE {clause} AND requeue_count >= ?",
        params + [MAX_REQUEUES],
    ).fetchone()[0]

    # "refetch": true discards the stored payload so the lead is fetched from Facebook again
    # (use after changing graph_lead_fields or fixing a token/permission problem).
    refetch = ", payload=NULL" if body.get("refetch") is True else ""
    now = now_iso()
    cur = conn.execute(
        "UPDATE leads SET status='pending', attempts=0, next_attempt_at=?, died_at=NULL,"
        f" requeue_count=requeue_count+1, updated_at=?{refetch}"
        f" WHERE {clause} AND requeue_count < ?",
        [now, now] + params + [MAX_REQUEUES],
    )
    log.info("Requeued %d dead lead(s) (%d skipped: requeue cap).", cur.rowcount, skipped)
    return jsonify({"requeued": cur.rowcount, "skipped_over_requeue_cap": skipped})


@app.route("/admin/stats", methods=["GET"])
@admin_required
def admin_stats():
    """Counts only (no lead data): early warning for expired tokens and stuck queues."""
    conn = db()
    counts = {s: 0 for s in ("pending", "processing", "done", "dead")}
    for r in conn.execute("SELECT status, COUNT(*) AS n FROM leads GROUP BY status"):
        counts[r["status"]] = r["n"]

    oldest = conn.execute(
        "SELECT MIN(created_at) FROM leads WHERE status IN ('pending','processing')"
    ).fetchone()[0]
    oldest_age = None
    if oldest:
        oldest_age = int((utcnow() - datetime.fromisoformat(oldest.replace("Z", "+00:00"))).total_seconds())

    hour_ago = iso(utcnow() - timedelta(hours=1))
    pages = {pid: {"name": cfg.get("name"), "last_success": None,
                   "failures_last_hour": 0, "top_error_class": None}
             for pid, cfg in PAGES.items()}

    for r in conn.execute("SELECT page_id, MAX(completed_at) AS last FROM leads"
                          " WHERE status='done' GROUP BY page_id"):
        if r["page_id"] in pages:
            pages[r["page_id"]]["last_success"] = r["last"]

    class_counts = {}
    for r in conn.execute(
        "SELECT l.page_id AS page_id, a.classification AS cls, COUNT(*) AS n"
        " FROM lead_attempts a JOIN leads l ON l.leadgen_id = a.leadgen_id"
        " WHERE a.success=0 AND a.created_at >= ? GROUP BY l.page_id, a.classification",
        (hour_ago,),
    ):
        if r["page_id"] in pages:
            pages[r["page_id"]]["failures_last_hour"] += r["n"]
            class_counts.setdefault(r["page_id"], {})[r["cls"]] = r["n"]
    for pid, classes in class_counts.items():
        pages[pid]["top_error_class"] = max(classes, key=classes.get)

    return jsonify({
        "counts": counts,
        "oldest_unfinished_age_seconds": oldest_age,
        "pages": pages,
        "workers_alive": sum(t.is_alive() for t in _worker_threads),
    })


@app.route("/health", methods=["GET"])
def health():
    try:
        db().execute("SELECT 1").fetchone()
    except sqlite3.Error:
        return jsonify({"status": "error"}), 503
    return jsonify({"status": "ok"})


# --------------------------------------------------------------------------------------
# Startup
# --------------------------------------------------------------------------------------

init_db()
if os.environ.get("DISABLE_WORKER") != "1":
    start_worker()

if __name__ == "__main__":
    # Development only. In production use gunicorn (see module docstring).
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)