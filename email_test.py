import os, json, hmac, hashlib, threading, tempfile, http.server, sys, time, sqlite3, email
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import AuthResult
inbox = []
class Handler:
    async def handle_DATA(self, server, session, envelope):
        inbox.append((envelope.rcpt_tos, email.message_from_bytes(envelope.content, policy=__import__("email.policy").policy.default))); return "250 OK"
def auth(server, session, envelope, mechanism, data):
    return AuthResult(success=(data.login == b"me@example.com" and data.password == b"right-pass-123"))
import socket as _s; _k=_s.socket(); _k.bind(("127.0.0.1",0)); _fp=_k.getsockname()[1]; _k.close()
smtp = Controller(Handler(), hostname="127.0.0.1", port=_fp, authenticator=auth, auth_require_tls=False); smtp.start()
sport = _fp

state = {"erp": "ok"}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, c, o):
        b = json.dumps(o).encode(); self.send_response(c); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        self._send(200, {"id": "x", "form_id": "F1", "field_data": [{"name": "full_name", "values": ["Asha K"]}, {"name": "phone_number", "values": ["+911234567890"]}]})
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        if state["erp"] == "err200": return self._send(200, {"message": {"status": "error", "error": "Value missing for Lead: Customer Name"}})
        if state["erp"] == "500": return self._send(500, {})
        self._send(200, {"message": {"status": "success"}})
srv = http.server.HTTPServer(("127.0.0.1", 0), H); threading.Thread(target=srv.serve_forever, daemon=True).start()
url = f"http://127.0.0.1:{srv.server_port}"
d = tempfile.mkdtemp()
ALERT = {"smtp_host": "127.0.0.1", "smtp_port": sport, "security": "none", "username": "me@example.com",
         "password": "right-pass-123", "to": "me@example.com"}
base = {"verify_token": "vt-123456", "app_secret": "appsecret1", "admin_api_key": "adminkey1", "graph_base_url": url,
        "erpnext": {"lead_url": url + "/lead"}, "pages": {"P1": {"name": "Main Page", "page_access_token": "TOKEN_ABCDEFG"}},
        "alert_email": ALERT}
def write_cfg(c):
    json.dump(c, open(d + "/c.json", "w")); t = time.time() + len(inbox) + write_cfg.n; write_cfg.n += 7; os.utime(d + "/c.json", (t, t))
write_cfg.n = 1
write_cfg(base)
os.environ.update(CREDENTIALS_PATH=d + "/c.json", DATABASE_PATH=d + "/t.db", DISABLE_WORKER="1", LOG_LEVEL="CRITICAL")
sys.path.insert(0, "."); import webhook as w
c = w.app.test_client(); A = {"X-Admin-Key": "adminkey1"}
fails = 0
def chk(name, ok):
    global fails; fails += (not ok); print(("PASS " if ok else "FAIL ") + name)
def wait_mail(n, t=5):
    end = time.time() + t
    while len(inbox) < n and time.time() < end: time.sleep(0.05)
    return len(inbox) >= n
def send(lid):
    body = json.dumps({"object": "page", "entry": [{"id": "P1", "changes": [{"field": "leadgen", "value": {"leadgen_id": lid, "page_id": "P1", "form_id": "F1"}}]}]}).encode()
    c.post("/webhook/facebook-leads", data=body, headers={"X-Hub-Signature-256": "sha256=" + hmac.new(b"appsecret1", body, hashlib.sha256).hexdigest()})

r = c.post("/admin/test-alert", headers=A); j = r.get_json()
chk("test-alert sends email", r.status_code == 200 and j["sent"] and wait_mail(1) and inbox[0][1]["Subject"] == "[FB Leads] Test alert")
chk("test-alert recipient", inbox[0][0] == ["me@example.com"])

state["erp"] = "err200"; send("L1"); w.run_once()
chk("dead lead emails", wait_mail(2))
rcpt, m = inbox[1]; body = m.get_content()
chk("subject names lead/page/class", m["Subject"] == "[FB Leads] Lead L1 failed - Main Page [permanent/push]")
chk("body has error + contact + advice + requeue",
    "Customer Name" in body and "Name: Asha K" in body and "Phone: +911234567890" in body and "Main Page (P1)" in body
    and "Attempts:    1" in body and "requeue" in body.lower() and '"leadgen_ids": ["L1"]' in body)
chk("no secrets in email", "TOKEN_ABCDEFG" not in body and "right-pass-123" not in body)

state["erp"] = "500"; send("L2"); w.run_once(); time.sleep(0.5)
chk("retryable failure does NOT email", len(inbox) == 2)
cn = sqlite3.connect(d + "/t.db")
for i in range(5):
    cn.execute("update leads set next_attempt_at='2000-01-01T00:00:00Z' where leadgen_id='L2'"); cn.commit(); w.run_once()
chk("6th failure emails once", wait_mail(3) and "Attempts:    6" in inbox[2][1].get_content() and "[transient/push]" in inbox[2][1]["Subject"])
time.sleep(0.5); chk("exactly one email per dead lead", len(inbox) == 3)

import socketserver
class Reject(socketserver.StreamRequestHandler):
    def handle(self):
        self.wfile.write(b"220 fake ESMTP\r\n")
        for line in self.rfile:
            cmd = line.strip().upper()
            if cmd.startswith((b"EHLO", b"HELO")): self.wfile.write(b"250-fake\r\n250 AUTH PLAIN LOGIN\r\n")
            elif cmd.startswith(b"AUTH"): self.wfile.write(b"535 5.7.8 Username and Password not accepted\r\n")
            elif cmd.startswith(b"QUIT"): self.wfile.write(b"221 bye\r\n"); return
            else: self.wfile.write(b"250 ok\r\n")
rej = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Reject); threading.Thread(target=rej.serve_forever, daemon=True).start()
bad = json.loads(json.dumps(base)); bad["alert_email"]["password"] = "wrong-pass-999"; bad["alert_email"]["smtp_port"] = rej.server_address[1]; write_cfg(bad)
r = c.post("/admin/test-alert", headers=A); j = r.get_json()
chk("wrong password -> 400 with reason", r.status_code == 400 and "SMTPAuthenticationError" in j["error"] and "not accepted" in j["error"] and "wrong-pass-999" not in j["error"])
dead = json.loads(json.dumps(base)); dead["alert_email"]["smtp_port"] = 1; write_cfg(dead)
r = c.post("/admin/test-alert", headers=A); chk("unreachable SMTP -> 400, no crash", r.status_code == 400 and r.get_json()["error"])
state["erp"] = "err200"; send("L3"); w.run_once(); time.sleep(1)
chk("SMTP down: lead still marked dead", cn.execute("select status from leads where leadgen_id='L3'").fetchone()[0] == "dead")

none = json.loads(json.dumps(base)); none.pop("alert_email"); write_cfg(none)
r = c.post("/admin/test-alert", headers=A); chk("no alert_email -> 400 not configured", r.status_code == 400 and "not configured" in r.get_json()["error"])
n = len(inbox); send("L4"); w.run_once(); time.sleep(0.5); chk("no alert_email -> no email, lead dead", len(inbox) == n)

for name, a in [("missing to", {"smtp_host": "h", "from": "a@b.c"}), ("bad security", {"smtp_host": "h", "to": "a@b.c", "from": "a@b.c", "security": "tls"}),
                ("user without password", {"smtp_host": "h", "to": "a@b.c", "username": "a@b.c"}), ("no host", {"to": "a@b.c", "from": "a@b.c"}),
                ("bad port", {"smtp_host": "h", "to": "a@b.c", "from": "a@b.c", "smtp_port": "abc"})]:
    try: w.validate_alert_email_cfg(a); chk("rejects " + name, False)
    except RuntimeError: chk("rejects " + name, True)
w.validate_alert_email_cfg({"smtp_host": "smtp.gmail.com", "to": ["a@b.c", "d@e.f"], "username": "a@b.c", "password": "x"}); chk("accepts list of recipients", True)

multi = json.loads(json.dumps(base)); multi["alert_email"]["to"] = ["me@example.com", "sales@example.com"]; write_cfg(multi)
n = len(inbox); c.post("/admin/test-alert", headers=A); chk("multiple recipients", wait_mail(n + 1) and sorted(inbox[-1][0]) == ["me@example.com", "sales@example.com"])
smtp.stop(); sys.exit(1 if fails else 0)
