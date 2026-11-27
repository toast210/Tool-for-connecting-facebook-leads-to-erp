"""Alert emails (SMTP) for dead leads."""
import json
import logging
import smtplib
import sqlite3
import ssl
import threading
from contextlib import closing
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

from .config import source_info
from .db import db, now_iso
from .security import redact

log = logging.getLogger("fb-leads")


SMTP_TIMEOUT = 20      # seconds


def send_email(cfg, subject, body):
    """Send a plain-text email using credentials.json 'alert_email'. Returns (ok, error)."""
    a = cfg.get("alert_email")
    if not a:
        return False, "alert_email is not configured in credentials.json"
    security = a.get("security", "starttls")
    port = int(a.get("smtp_port") or (465 if security == "ssl" else 587 if security == "starttls" else 25))
    to = [a["to"]] if isinstance(a["to"], str) else list(a["to"])
    sender = a.get("from") or a["username"]

    msg = EmailMessage()
    msg["Subject"] = f"{a.get('subject_prefix', '[FB Leads]')} {subject}".strip()
    msg["From"] = sender
    msg["To"] = ", ".join(to)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=sender.rpartition("@")[2] or None)
    msg.set_content(body)
    try:
        ctx = ssl.create_default_context()
        if security == "ssl":
            server = smtplib.SMTP_SSL(a["smtp_host"], port, timeout=SMTP_TIMEOUT, context=ctx)
        else:
            server = smtplib.SMTP(a["smtp_host"], port, timeout=SMTP_TIMEOUT)
        with server:
            if security == "starttls":
                server.starttls(context=ctx)
            if a.get("username"):
                server.login(a["username"], a["password"])
            server.send_message(msg, from_addr=sender, to_addrs=to)
        return True, None
    except (smtplib.SMTPException, OSError) as e:
        return False, redact(f"{type(e).__name__}: {e}", 300)


def _lead_contact(lead_id):
    """Name/phone/email from the stored payload, so the email can be acted on directly."""
    try:
        with closing(db()) as conn:
            r = conn.execute("SELECT payload FROM leads WHERE id=?", (lead_id,)).fetchone()
        p = json.loads(r["payload"]) if r and r["payload"] else {}
    except (sqlite3.Error, ValueError):
        p = {}
    return {k: p.get(k) for k in ("Name", "Phone", "Email") if p.get(k)}


def build_alert_email(cfg, row, err, attempts):
    origin = source_info(cfg, row["page_id"])
    if origin["website"]:
        page_label = origin["name"]
    else:
        page_label = f"{origin['name']} ({row['page_id']})" if origin["name"] else str(row["page_id"])
    contact = _lead_contact(row["id"])
    subject = f"Lead {row['leadgen_id']} failed - {origin['name'] or row['page_id']} [{err.cls}/{err.stage}]"
    lines = [
        f"A {'website' if origin['website'] else 'Facebook'} lead could not be delivered to ERPNext "
        "and the service has stopped retrying it.",
        "",
        f"Lead ID:     {row['leadgen_id']}",
        f"{'Source:' if origin['website'] else 'Page:'}{' ' * (5 if origin['website'] else 7)}{page_label}",
        f"Form ID:     {row['form_id'] or '-'}",
        f"Received:    {row['created_at']} (UTC)",
        f"Failed at:   {now_iso()} (UTC)",
        f"Attempts:    {attempts}",
        f"Stage:       {err.stage}   ({ {'fetch': 'getting the lead from Facebook', 'map': 'building the ERPNext payload', 'push': 'sending to ERPNext'}.get(err.stage, err.stage) })",
        f"Error class: {err.cls}",
        f"HTTP status: {err.http_status or '-'}",
        "",
        "Error:",
        redact(err.message, 1500),
    ]
    if contact:
        lines += ["", "Lead contact (so you can follow up by hand):"]
        lines += [f"  {k}: {v}" for k, v in contact.items()]
    lines += ["", "What to do:", {
        "token": "  The page access token is expired or lacks permission. Put a new token for this page in "
                 "credentials.json, then requeue the page's dead leads.",
        "transient": "  Facebook or ERPNext was unreachable for the whole retry window (~31 min). Once it is "
                     "back, requeue this lead.",
        "permanent": "  ERPNext or Facebook rejected the lead. Fix the cause shown above (often a mandatory "
                     "field or a mapping), then requeue this lead.",
    }.get(err.cls, "  Fix the cause above, then requeue this lead.")]
    lines += ["", "Requeue it with:",
              f'  curl -X POST <service-url>/admin/requeue -H "X-Admin-Key: <admin key>" '
              f'-H "Content-Type: application/json" -d \'{{"leadgen_ids": ["{row["leadgen_id"]}"]}}\'']
    return subject, "\n".join(lines) + "\n"


def send_alert(cfg, row, err, attempts):
    """Email the 'lead is dead' alert in the background so the queue keeps moving."""
    if not cfg.get("alert_email"):
        return
    try:
        subject, body = build_alert_email(cfg, row, err, attempts)
    except Exception:
        log.exception("could not build alert email for lead %s", row["leadgen_id"])
        return

    def _send():
        ok, why = send_email(cfg, subject, body)
        if ok:
            log.info("alert email sent for lead %s", row["leadgen_id"])
        else:
            log.warning("alert email for lead %s failed: %s", row["leadgen_id"], why)
    threading.Thread(target=_send, name="alert-email", daemon=True).start()
