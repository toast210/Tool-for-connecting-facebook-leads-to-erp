"""Facebook Lead Ads webhook: the handshake and leadgen notifications."""
import hmac
import json
import logging
import sqlite3
from contextlib import closing

from flask import Blueprint, jsonify, request

from ..config import get_config
from ..db import db, now_iso
from ..security import valid_signature

log = logging.getLogger("fb-leads")
bp = Blueprint("facebook", __name__)


@bp.get("/webhook/facebook-leads")
def verify_webhook():
    cfg = get_config()
    token = request.args.get("hub.verify_token", "")
    if request.args.get("hub.mode") == "subscribe" and hmac.compare_digest(token.encode(), cfg["verify_token"].encode()):
        return request.args.get("hub.challenge", ""), 200
    return "Forbidden", 403


@bp.post("/webhook/facebook-leads")
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
    if not isinstance(body, dict):
        # 4xx, not 5xx, so Meta does not keep retrying a body that can never be processed.
        return "Expected a JSON object", 400

    try:
        return _queue_leads(body, cfg)
    except sqlite3.Error:
        log.exception("webhook: database error")
        return jsonify({"status": "error"}), 500


def _queue_leads(body, cfg):
    now = now_iso()
    queued = duplicates = skipped = 0
    with closing(db()) as conn:
        entries = body.get("entry")
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            changes = entry.get("changes")
            for change in changes if isinstance(changes, list) else []:
                if not isinstance(change, dict) or change.get("field") != "leadgen":
                    continue
                v = change.get("value")
                if not isinstance(v, dict):
                    continue
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
