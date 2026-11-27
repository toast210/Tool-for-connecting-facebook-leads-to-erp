"""Tests for POST /webhook/website-leads. Runs locally with a mock ERPNext; exits 1 on any failure.

    python website_test.py
"""
import http.server
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time

d = tempfile.mkdtemp()
state = {"erp": "ok", "received": []}


class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):          # a website lead must never call the Graph API
        state["graph_called"] = True
        self._send(500, {})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        state["received"].append(body)
        if state["erp"] == "500":
            return self._send(500, {})
        if state["erp"] == "err200":
            return self._send(200, {"message": {"status": "error", "error": "Value missing for Lead: Mobile"}})
        self._send(200, {"message": {"name": f"CRM-LEAD-{len(state['received']):05d}"}})


srv = http.server.HTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
url = f"http://127.0.0.1:{srv.server_port}"
KEY1, KEY2 = "website-key-one-1234567890", "website-key-two-0987654321"
BASE = {"verify_token": "vt-123456", "app_secret": "appsecret1", "admin_api_key": "adminkey1",
        "graph_base_url": url, "erpnext": {"lead_url": url + "/lead"},
        "pages": {"P1": {"page_access_token": "TOKEN_ABCDEFG", "name": "Main Page"}}}


def write_cfg(cfg, n=[0]):
    json.dump(cfg, open(d + "/c.json", "w"))
    n[0] += 1
    t = time.time() + n[0] * 5
    os.utime(d + "/c.json", (t, t))


write_cfg(BASE)       # first without a 'website' section: endpoint disabled
os.environ.update(CREDENTIALS_PATH=d + "/c.json", DATABASE_PATH=d + "/t.db", DISABLE_WORKER="1",
                  LOG_LEVEL="CRITICAL")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import webhook as w  # noqa: E402

c = w.app.test_client()
A = {"X-Admin-Key": "adminkey1"}
EP = "/webhook/website-leads"
FAILS = []


def chk(name, ok, extra=""):
    print(("PASS " if ok else "FAIL ") + name + (f"  [{extra}]" if extra and not ok else ""))
    if not ok:
        FAILS.append(name)


def row(lid):
    return sqlite3.connect(d + "/t.db").execute(
        "select status,attempts,last_error_class,page_id,form_id from leads where leadgen_id=?", (lid,)).fetchone()


def post(body, key=KEY1, header="X-Website-Key", **kw):
    h = {header: (f"Bearer {key}" if header == "Authorization" else key)} if key else {}
    return c.post(EP, json=body, headers=h, **kw)


# --- disabled until configured
chk("no 'website' section -> 404", post({"phone": "1"}).status_code == 404)
write_cfg(dict(BASE, website={"api_keys": [KEY1, KEY2], "source": "Website - Contact form", "name": "Website"}))

# --- authentication
chk("no key -> 401", post({"phone": "1"}, key=None).status_code == 401)
chk("wrong key -> 401", post({"phone": "1"}, key="x" * 30).status_code == 401)
r = post({"phone": "+91 90000 00001", "submission_id": "auth-1"}, header="Authorization")
chk("Authorization: Bearer key accepted", r.status_code == 200, r.get_json())
r = post({"phone": "+91 90000 00002", "submission_id": "auth-2"}, key=KEY2)
chk("second key in api_keys accepted (rotation)", r.status_code == 200)

# --- a normal JSON submission, end to end
sub = {"submission_id": "1001", "form_name": "contact-us", "name": "  Ásha   Kaur ", "phone": "+91 98765-43210",
       "email": " Asha@Example.COM ", "city": "Ludhiana", "message": "Need a quote for 20 units",
       "interested_in": ["Solar", "Inverter"], "newsletter": True}
r = post(sub)
j = r.get_json()
chk("JSON lead -> 200 queued", r.status_code == 200 and j["lead_id"] == "web-1001" and j["duplicate"] is False, j)
chk("stored under page 'website' with form id", row("web-1001")[3:] == ("website", "contact-us"))
state["received"].clear()
w.run_once()
p = next((x for x in state["received"] if x.get("leadgen_id") == "web-1001"), {})
chk("delivered to ERPNext -> done", row("web-1001")[0] == "done", row("web-1001"))
chk("Graph API never called", not state.get("graph_called"))
chk("mapped + normalised", p.get("Name") == "Ásha Kaur" and p.get("Phone") == "+919876543210"
    and p.get("Email") == "asha@example.com", p)
chk("other fields passed through", p.get("City") == "Ludhiana" and p.get("Message") == "Need a quote for 20 units"
    and p.get("Interested In") == "Solar, Inverter" and p.get("Newsletter") == "Yes", p)
chk("source + website metadata", p.get("Lead Source") == "Website - Contact form" and p.get("Page ID") == "website"
    and p.get("Form ID") == "contact-us" and p.get("Platform") == "website" and p.get("Created Time"), p)
chk("no empty ad/campaign fields", not any(k in p for k in ("Ad Name", "Adset Name", "Campaign", "Ad ID",
                                                            "Campaign ID", "Is Organic")), list(p))
chk("id/form keys not duplicated as answers", "Submission Id" not in p and "Form Name" not in p, list(p))

# --- duplicates and ids
r = post(sub)
chk("same submission_id again -> duplicate, not re-sent", r.get_json()["duplicate"] is True)
state["received"].clear()
w.run_once()
chk("duplicate not delivered twice", not any(x.get("leadgen_id") == "web-1001" for x in state["received"]))
a = post({"email": "a@b.co"}).get_json()["lead_id"]
b = post({"email": "a@b.co"}).get_json()["lead_id"]
chk("no id -> a fresh id each time", a != b and a.startswith("web-") and b.startswith("web-"))
odd = post({"email": "a@b.co", "lead_id": "weird id / with spaces"}).get_json()["lead_id"]
chk("unsafe id characters -> hashed id", odd.startswith("web-") and " " not in odd and "/" not in odd, odd)

# --- form post (application/x-www-form-urlencoded)
r = c.post(EP, data={"name": "Ravi", "mobile": "09876543210", "submission_id": "f-1"}, headers={"X-Website-Key": KEY1})
state["received"].clear()
w.run_once()
p = next((x for x in state["received"] if x.get("leadgen_id") == "web-f-1"), {})
chk("HTML form post accepted", r.status_code == 200 and p.get("Name") == "Ravi" and p.get("Phone") == "09876543210", p)

# --- validation
r = post({"name": "No Contact", "city": "Delhi"})
chk("no phone or email -> 400 with accepted field names", r.status_code == 400
    and "phone_number" in r.get_json()["accepted_phone_fields"])
chk("invalid JSON -> 400", c.post(EP, data=b"{bad", headers={"X-Website-Key": KEY1,
                                                             "Content-Type": "application/json"}).status_code == 400)
chk("JSON array -> 400", c.post(EP, data=b"[]", headers={"X-Website-Key": KEY1,
                                                         "Content-Type": "application/json"}).status_code == 400)
chk("empty body -> 400", c.post(EP, headers={"X-Website-Key": KEY1}).status_code == 400)
chk("too many fields -> 400", post(dict({f"f{i}": "x" for i in range(120)}, phone="1")).status_code == 400)
chk("> 1 MB body -> 413", c.post(EP, data=b"x" * 1_100_000, headers={"X-Website-Key": KEY1,
                                                                     "Content-Type": "application/json"}).status_code == 413)

# --- custom field mapping also applies to the website
write_cfg(dict(BASE, website={"api_key": KEY1}, payload={"field_mapping": {"Phone": ["contact_no"]}}))
r = post({"contact_no": "1234567890", "submission_id": "map-1"})
state["received"].clear()
w.run_once()
p = next((x for x in state["received"] if x.get("leadgen_id") == "web-map-1"), {})
chk("payload.field_mapping used for website leads", r.status_code == 200 and p.get("Phone") == "1234567890"
    and p.get("Lead Source") == "Website", p)

# --- failures: retries, dead, alert text, requeue keeps the data
write_cfg(dict(BASE, website={"api_key": KEY1}))
state["erp"] = "500"
post({"phone": "111", "submission_id": "retry-1"})
w.run_once()
chk("ERPNext down -> retried later (pending)", row("web-retry-1")[:3] == ("pending", 1, "transient"), row("web-retry-1"))
state["erp"] = "err200"
post({"phone": "222", "name": "Dead Lead", "submission_id": "dead-1"})
w.run_once()
chk("ERPNext rejects -> dead", row("web-dead-1")[0] == "dead", row("web-dead-1"))
con = sqlite3.connect(d + "/t.db")
con.row_factory = sqlite3.Row
dr = con.execute("select * from leads where leadgen_id='web-dead-1'").fetchone()
subj, body = w.build_alert_email(w.get_config(), dr, w.LeadError("push", "permanent", "Value missing"), 1)
chk("alert email names the website", "website lead" in body and "Source:     Website" in body
    and "Website [permanent/push]" in subj and "Name: Dead Lead" in body, subj + "\n" + body)
state["erp"] = "ok"
rq = c.post("/admin/requeue", json={"leadgen_ids": ["web-dead-1"], "refetch": True}, headers=A).get_json()
w.run_once()
chk("requeue with refetch keeps website data -> done", rq["requeued"] == 1 and row("web-dead-1")[0] == "done"
    and not state.get("graph_called"), row("web-dead-1"))
st = c.get("/admin/stats", headers=A).get_json()
chk("stats list 'website'", "website" in st["pages"] and st["pages"]["website"]["last_success"], st["pages"])
dl = c.get("/admin/dead-leads?page_id=website", headers=A).get_json()
chk("dead-lead filter works for website", dl["total"] == 0)

# --- website-only field mapping (the real contact form's fields)
SITE_MAP = {"City": ["city"], "Profession": ["role"], "Project Type": ["projectType"], "Interested In": ["interest"]}
write_cfg(dict(BASE, website={"api_key": KEY1, "field_mapping": SITE_MAP}))
form = {"submission_id": "site-1", "name": "Rohit Sharma", "mobile": "+91 98765 43210", "email": "Rohit@Example.com",
        "city": "Mohali", "role": "Architect", "projectType": "Residential", "interest": ["Solar", "Lighting"]}
r = post(form)
state["received"].clear()
w.run_once()
p = next((x for x in state["received"] if x.get("leadgen_id") == "web-site-1"), {})
want = {"Name": "Rohit Sharma", "Phone": "+919876543210", "Email": "rohit@example.com", "City": "Mohali",
        "Profession": "Architect", "Project Type": "Residential", "Interested In": "Solar, Lighting"}
chk("website field_mapping: contact form mapped", r.status_code == 200 and all(p.get(k) == v for k, v in want.items()), p)
chk("website field_mapping: no raw duplicates", not any(k in p for k in ("Role", "ProjectType", "Interest", "Mobile")),
    list(p))
r = post({"mobile": "9876543210", "submission_id": "site-2"})
state["received"].clear()
w.run_once()
p = next((x for x in state["received"] if x.get("leadgen_id") == "web-site-2"), {})
chk("website field_mapping: missing answers sent empty", p.get("Phone") == "9876543210" and p.get("Profession") == "", p)
fb_row = {"id": 0, "leadgen_id": "fb-1", "page_id": "P1", "webhook_data": json.dumps({"form_id": "F1"})}
fb = w.build_payload(fb_row, {"field_data": [{"name": "full_name", "values": ["FB Person"]},
                                             {"name": "role", "values": ["Builder"]}]}, w.get_config())
chk("website field_mapping does not touch Facebook leads",
    fb.get("Name") == "FB Person" and "Profession" not in fb and "Project Type" not in fb and fb.get("Role") == "Builder",
    fb)
write_cfg(dict(BASE, website={"api_key": KEY1, "field_mapping": {"City": 5}}))
try:
    w._load_config_file()
    chk("rejects bad website.field_mapping", False)
except RuntimeError:
    chk("rejects bad website.field_mapping", True)

# --- config validation and secrets
for name, cfg in [("short key", dict(BASE, website={"api_key": "short"})),
                  ("no key", dict(BASE, website={"source": "Website"})),
                  ("'website' as a page id", dict(BASE, pages={"website": {}}, website={"api_key": KEY1})),
                  ("keys not a list", dict(BASE, website={"api_keys": KEY1}))]:
    write_cfg(cfg)
    try:
        w._load_config_file()
        chk("rejects " + name, False)
    except RuntimeError:
        chk("rejects " + name, True)
write_cfg(dict(BASE, website={"api_key": KEY1}))
w.get_config()
chk("website key redacted from logs/errors", KEY1 not in w.redact(f"boom {KEY1}"))

print(f"\n{len(FAILS)} failed" if FAILS else "\nall checks passed")
sys.exit(1 if FAILS else 0)
