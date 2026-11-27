"""Website form leads: POST /webhook/website-leads."""
import hashlib
import hmac
import json
import logging
import os
import re
import sqlite3
from contextlib import closing

from flask import Blueprint, jsonify, request

from ..config import (WEBSITE_FORM_KEYS, WEBSITE_ID_KEYS, WEBSITE_MAX_FIELDS, WEBSITE_MAX_VALUE, WEBSITE_PAGE_ID,
                      get_config, website_keys)
from ..db import db, now_iso
from ..mapping import norm_key, payload_settings

log = logging.getLogger("fb-leads")
bp = Blueprint("website", __name__)


# ---------------------------------------------------------------- website form leads
def _website_key_ok(cfg):
    sent = request.headers.get("X-Website-Key", "")
    auth = request.headers.get("Authorization", "")
    if not sent and auth[:7].lower() == "bearer ":
        sent = auth[7:].strip()
    if not sent:
        return False
    return any(hmac.compare_digest(sent.encode(), k.encode()) for k in website_keys(cfg))


def _website_value(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "Yes" if v else "No"
    if isinstance(v, list):
        return ", ".join(_website_value(x) for x in v if _website_value(x))
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return str(v).strip()


def _website_body():
    """The submission as a flat dict: a JSON object, or a normal HTML form post."""
    if request.is_json:
        body = request.get_json(silent=True)
        return body if isinstance(body, dict) else None
    if request.form:
        return {k: (v[0] if len(v) == 1 else v) for k, v in request.form.to_dict(flat=False).items()}
    return None


def _website_lead_id(body):
    for k in WEBSITE_ID_KEYS:
        v = _website_value(body.get(k))
        if v:
            if re.fullmatch(r"[A-Za-z0-9_.:\-]{1,100}", v):
                return f"web-{v}"
            return "web-" + hashlib.sha256(v.encode()).hexdigest()[:32]
    return f"web-{os.urandom(16).hex()}"


@bp.post("/webhook/website-leads")
def receive_website_lead():
    """One website form submission -> the same queue as Facebook leads (already holding its data)."""
    cfg = get_config()
    if not website_keys(cfg):
        return jsonify({"error": "website endpoint is not enabled (no 'website' section in credentials.json)"}), 404
    if not _website_key_ok(cfg):
        log.warning("website lead rejected: missing or wrong API key")
        return jsonify({"error": "unauthorized"}), 401
    body = _website_body()
    if body is None:
        return jsonify({"error": "send a JSON object (Content-Type: application/json) or a form post"}), 400

    reserved = {norm_key(k) for k in WEBSITE_ID_KEYS + WEBSITE_FORM_KEYS}
    form_id = next((_website_value(body.get(k)) for k in WEBSITE_FORM_KEYS if _website_value(body.get(k))), "")
    field_data = []
    for k, v in body.items():
        key = str(k).strip()[:100]
        if not key or norm_key(key) in reserved:
            continue
        val = _website_value(v)[:WEBSITE_MAX_VALUE]
        if val:
            field_data.append({"name": key, "values": [val]})
    if len(field_data) > WEBSITE_MAX_FIELDS:
        return jsonify({"error": f"too many fields (max {WEBSITE_MAX_FIELDS})"}), 400

    # the lead must be reachable: a phone or an email under any name the mapping knows
    mapping = payload_settings(cfg, website=True)["mapping"]
    contact_keys = {norm_key(s) for out in ("Phone", "Email") for s in mapping.get(out, [])}
    if not any(norm_key(f["name"]) in contact_keys for f in field_data):
        return jsonify({"error": "a phone number or email is required",
                        "accepted_phone_fields": mapping.get("Phone", []),
                        "accepted_email_fields": mapping.get("Email", [])}), 400

    lid = _website_lead_id(body)
    now = now_iso()
    graph = {"id": lid, "created_time": now, "form_id": form_id, "platform": "website", "field_data": field_data}
    meta = {"source": "website", "form_id": form_id, "created_time": now,
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()}
    try:
        with closing(db()) as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO leads(leadgen_id,page_id,form_id,status,attempts,next_attempt_at,"
                "graph_data,webhook_data,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (lid, WEBSITE_PAGE_ID, form_id, "pending", 0, now, json.dumps(graph), json.dumps(meta), now, now))
    except sqlite3.Error:
        log.exception("website lead: database error")
        return jsonify({"status": "error"}), 500
    duplicate = cur.rowcount == 0
    log.info("Website: lead %s %s", lid, "duplicate (already queued)" if duplicate else "queued")
    return jsonify({"status": "ok", "lead_id": lid, "duplicate": duplicate}), 200
