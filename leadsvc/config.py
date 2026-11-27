"""Constants and credentials.json loading, validation and live reload."""
import json
import logging
import os
import threading

log = logging.getLogger("fb-leads")


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
    "Email": ["email", "email_address", "work_email"]
}
# Graph/webhook field -> output name. Order matters: the first three are sent right after the answers.
DEFAULT_META_NAMES = {
    "ad_name": "Ad Name", "adset_name": "Adset Name", "campaign_name": "Campaign",
    "ad_id": "Ad ID", "adset_id": "Adset ID", "campaign_id": "Campaign ID", "form_id": "Form ID",
    "created_time": "Created Time", "platform": "Platform", "is_organic": "Is Organic",
    "page_id": "Page ID", "lead_source": "Lead Source",
}
PRIMARY_META = ("ad_name", "adset_name", "campaign_name")

# Website form leads (POST /webhook/website-leads) are stored under this pseudo page id.
WEBSITE_PAGE_ID = "website"
WEBSITE_ID_KEYS = ("submission_id", "lead_id", "id")       # first one present = dedupe id
WEBSITE_FORM_KEYS = ("form_name", "form_id", "form")       # first one present = "Form ID"
WEBSITE_META = ("form_id", "created_time", "platform", "page_id", "lead_source")
WEBSITE_MAX_FIELDS = 100
WEBSITE_MAX_VALUE = 5000

GRAPH_TOKEN_CODES = {10, 102, 190} | set(range(200, 300))
GRAPH_TRANSIENT_CODES = {1, 2, 4, 17, 32, 341, 613} | set(range(80000, 80015))


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
    validate_alert_email_cfg(data.get("alert_email"))
    validate_website_cfg(data.get("website"))
    if WEBSITE_PAGE_ID in data["pages"]:
        raise RuntimeError(f"credentials.json: '{WEBSITE_PAGE_ID}' is reserved for website leads; "
                           "it cannot be a Facebook Page id")
    if data.get("alert_webhook_url"):
        log.warning("credentials.json: 'alert_webhook_url' is no longer used; alerts are sent by email "
                    "(see 'alert_email' in README.md)")
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


def valid_field_mapping(fm):
    """{output name: form key or [form keys]}"""
    return isinstance(fm, dict) and all(
        isinstance(k, str) and (isinstance(v, str) or (isinstance(v, list) and all(isinstance(x, str) for x in v)))
        for k, v in fm.items())


def validate_payload_cfg(p):
    def bad(msg):
        raise RuntimeError(f"credentials.json payload: {msg}")
    if not isinstance(p, dict):
        bad("must be an object")
    if not valid_field_mapping(p.get("field_mapping", {})):
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


SMTP_SECURITY = ("starttls", "ssl", "none")


def validate_alert_email_cfg(a):
    """'alert_email' is optional; when present it must be usable."""
    if a is None:
        return

    def bad(msg):
        raise RuntimeError(f"credentials.json alert_email: {msg}")
    if not isinstance(a, dict):
        bad("must be an object")
    if not a.get("smtp_host"):
        bad("'smtp_host' is required")
    to = a.get("to")
    if not to or not (isinstance(to, str) or (isinstance(to, list) and all(isinstance(x, str) and x for x in to))):
        bad("'to' must be an email address or a list of addresses")
    if not (a.get("from") or a.get("username")):
        bad("'from' is required (or set 'username', which is then used as the sender)")
    sec = a.get("security", "starttls")
    if sec not in SMTP_SECURITY:
        bad(f"'security' must be one of {list(SMTP_SECURITY)}")
    if "smtp_port" in a:
        try:
            int(a["smtp_port"])
        except (TypeError, ValueError):
            bad("'smtp_port' must be a number")
    if bool(a.get("username")) != bool(a.get("password")):
        bad("set both 'username' and 'password', or neither")


def website_keys(cfg):
    w = cfg.get("website") or {}
    keys = w.get("api_keys") or ([w["api_key"]] if w.get("api_key") else [])
    return [k for k in keys if isinstance(k, str) and k]


def validate_website_cfg(w):
    """'website' is optional; when present the endpoint is enabled and needs at least one key."""
    if w is None:
        return

    def bad(msg):
        raise RuntimeError(f"credentials.json website: {msg}")
    if not isinstance(w, dict):
        bad("must be an object")
    if "api_keys" in w and not (isinstance(w["api_keys"], list) and all(isinstance(k, str) for k in w["api_keys"])):
        bad("'api_keys' must be a list of strings")
    if "api_key" in w and not isinstance(w["api_key"], str):
        bad("'api_key' must be a string")
    keys = website_keys({"website": w})
    if not keys:
        bad("set 'api_key' (or 'api_keys' to rotate keys)")
    if any(len(k) < 16 for k in keys):
        bad("each API key must be at least 16 characters; generate one with "
            "python3 -c \"import secrets; print(secrets.token_urlsafe(32))\"")
    for key in ("source", "name"):
        if key in w and not isinstance(w[key], str):
            bad(f"'{key}' must be a string")
    if "field_mapping" in w and not valid_field_mapping(w["field_mapping"]):
        bad("field_mapping must map an output name to a form field name or a list of names")


def source_info(cfg, page_id):
    """Display name and Lead Source for a lead's origin (a Facebook Page or the website)."""
    page_id = str(page_id)
    if page_id == WEBSITE_PAGE_ID:
        w = cfg.get("website") or {}
        return {"name": w.get("name", "Website"), "source": w.get("source", "Website"), "website": True}
    p = cfg["pages"].get(page_id, {})
    return {"name": p.get("name"), "source": p.get("source", "Facebook Lead Ads"), "website": False}
