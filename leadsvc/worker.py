"""Queue worker: claim due leads, fetch -> map -> push, retries, dead leads and housekeeping."""
import json
import logging
import os
import random
import socket
import threading
import time
from contextlib import closing
from datetime import timedelta

from .alerts import send_alert
from .config import (BACKOFF_BASE_SECONDS, BATCH_SIZE, DEAD_RETENTION_DAYS, DONE_RETENTION_DAYS,
                     MAINTENANCE_EVERY_SECONDS, MAX_RETRIES, STALE_PROCESSING_SECONDS, WEBSITE_PAGE_ID,
                     WORKER_IDLE_SECONDS, get_config, source_info)
from .db import db, iso, now_dt, now_iso, record_attempt
from .erpnext import erp_created_name, push_lead
from .errors import LeadError
from .graph import fetch_lead
from .mapping import build_payload
from .security import redact

log = logging.getLogger("fb-leads")


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
            elif str(row["page_id"]) == WEBSITE_PAGE_ID:
                raise LeadError("fetch", "permanent", "Website lead has no stored form data")
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
                 source_info(cfg, row["page_id"])["name"] or row["page_id"])
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
        send_alert(cfg, row, err, attempt_no)


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
