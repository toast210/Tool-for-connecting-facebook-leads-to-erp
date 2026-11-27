"""Secret redaction, the Facebook signature check and the admin-key guard."""
import hashlib
import hmac
import re
from functools import wraps

from flask import jsonify, request

from .config import get_config, website_keys


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
    vals.append((cfg.get("alert_email") or {}).get("password"))
    vals += website_keys(cfg)
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


def valid_signature(raw, header, secret):
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected.encode(), header[len("sha256="):].encode())


def admin_required(fn):
    @wraps(fn)
    def wrapper(*a, **kw):
        key = request.headers.get("X-Admin-Key", "")
        if not hmac.compare_digest(key.encode(), get_config()["admin_api_key"].encode()):
            return jsonify({"error": "unauthorized"}), 401
        return fn(*a, **kw)
    return wrapper
