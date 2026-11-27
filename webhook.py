"""Facebook Lead Ads (and website form) -> ERPNext webhook.

Receives Meta leadgen webhooks, queues the lead ID in SQLite, and a background
worker fetches the full lead from the Graph API and POSTs it to ERPNext, with
retries. Website form submissions arrive at /webhook/website-leads with their data
and go through the same queue (no Graph fetch). When a lead gives up ("dead") an
alert email is sent. See README.md for setup.

The code lives in the leadsvc package; this file builds the Flask app, checks the
configuration, prepares the database and starts the worker thread(s). Servers run it
as `webhook:app`.
"""
import logging
import os

from flask import Flask

from leadsvc.config import DATABASE_PATH, PORT, get_config
from leadsvc.db import init_db
from leadsvc.routes import admin, facebook, website
from leadsvc.worker import start_workers

# Re-exported so `import webhook` keeps working for the tests and any existing scripts.
from leadsvc.alerts import build_alert_email, send_alert, send_email
from leadsvc.config import _load_config_file, validate_alert_email_cfg, validate_payload_cfg, validate_website_cfg
from leadsvc.db import db
from leadsvc.errors import LeadError
from leadsvc.mapping import build_payload, payload_settings
from leadsvc.security import redact
from leadsvc.worker import process_lead, run_once

__all__ = ["app", "get_config", "init_db", "start_workers",
           "build_alert_email", "send_alert", "send_email",
           "_load_config_file", "validate_alert_email_cfg", "validate_payload_cfg", "validate_website_cfg",
           "db", "LeadError", "build_payload", "payload_settings", "redact", "process_lead", "run_once"]

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")
log = logging.getLogger("fb-leads")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1_000_000   # webhooks are tiny; reject anything large
app.register_blueprint(facebook.bp)
app.register_blueprint(website.bp)
app.register_blueprint(admin.bp)


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
