"""The public health check and the admin API (X-Admin-Key)."""
import json
import socket
from contextlib import closing
from datetime import timedelta

from flask import Blueprint, jsonify, request

from ..alerts import send_email
from ..config import HEARTBEAT_ALIVE_SECONDS, WEBSITE_PAGE_ID, get_config, website_keys
from ..db import db, iso, now_dt, now_iso, parse_iso
from ..security import admin_required, redact

bp = Blueprint("admin", __name__)


@bp.get("/health")
def health():
    try:
        with closing(db()) as conn:
            conn.execute("SELECT 1").fetchone()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": redact(e)}), 503


@bp.get("/admin/stats")
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
    cfg = get_config()
    for pid in list(cfg["pages"]) + ([WEBSITE_PAGE_ID] if website_keys(cfg) else []):
        pages.setdefault(str(pid), {"last_success": None, "failures_last_hour": 0})
    return jsonify({"counts": counts, "oldest_unfinished_age_seconds": age,
                    "pages": pages, "workers_alive": alive})


@bp.post("/admin/test-alert")
@admin_required
def admin_test_alert():
    cfg = get_config()
    to = (cfg.get("alert_email") or {}).get("to")
    ok, why = send_email(cfg, "Test alert", (
        "This is a test email from the Facebook Lead Ads -> ERPNext webhook.\n\n"
        "If you received it, alert emails are set up correctly: you will get one like this whenever a "
        f"lead fails for good.\n\nSent at {now_iso()} (UTC) from host {socket.gethostname()}.\n"))
    return jsonify({"sent": ok, "to": to, "error": why}), (200 if ok else 400)


@bp.get("/admin/dead-leads")
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


@bp.get("/admin/dead-leads/<leadgen_id>")
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


@bp.post("/admin/requeue")
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
                    "updated_at=?, dead_at=NULL, locked_at=NULL, "
                    # a website lead's stored form data is its only copy: never discarded by refetch
                    "payload=CASE WHEN ? AND page_id<>? THEN NULL ELSE payload END, "
                    "graph_data=CASE WHEN ? AND page_id<>? THEN NULL ELSE graph_data END WHERE id=?",
                    (now, now, 1 if refetch else 0, WEBSITE_PAGE_ID, 1 if refetch else 0, WEBSITE_PAGE_ID,
                     r["id"]))
                requeued.append(r["leadgen_id"])
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK"); raise
    return jsonify({"requeued": len(requeued), "skipped_over_max_requeues": skipped, "leadgen_ids": requeued})
