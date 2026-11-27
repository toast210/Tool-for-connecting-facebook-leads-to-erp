import os, json, hmac, hashlib, threading, tempfile, http.server, sys, time
d = tempfile.mkdtemp()
state = {"graph": "ok", "erp": "ok"}
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def _send(self, code, obj):
        b=json.dumps(obj).encode(); self.send_response(code); self.send_header("Content-Type","application/json"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if state["graph"]=="190": return self._send(400,{"error":{"message":"expired","code":190}})
        self._send(200,{"id":"L1","created_time":"2026-10-30T10:00:00+0000","form_id":"F1","platform":"fb",
          "field_data":[{"name":"full_name","values":["Asha K"]},{"name":"phone_number","values":["+911234567890"]}],
          "custom_disclaimer_responses":[{"checkbox_key":"consent","status":"checked"}]})
    def do_POST(self):
        n=int(self.headers["Content-Length"]); body=json.loads(self.rfile.read(n)); state["last"]=body
        m=state["erp"]
        if m=="ok": return self._send(200,{"message":{"status":"success"}})
        if m=="err200": return self._send(200,{"message":{"status":"error","error":"Value missing for Lead: Customer Name"}})
        if m=="dup": return self._send(409,{"exc_type":"DuplicateEntryError"})
        if m=="500": return self._send(500,{})
srv=http.server.HTTPServer(("127.0.0.1",0),H); threading.Thread(target=srv.serve_forever,daemon=True).start()
url=f"http://127.0.0.1:{srv.server_port}"
json.dump({"verify_token":"vt-123456","app_secret":"appsecret1","admin_api_key":"adminkey1",
 "graph_base_url":url,"erpnext":{"lead_url":url+"/lead"},
 "pages":{"P1":{"page_access_token":"TOKEN_ABCDEFG","source":"FB Ads"}}}, open(d+"/c.json","w"))
os.environ.update(CREDENTIALS_PATH=d+"/c.json", DATABASE_PATH=d+"/t.db", DISABLE_WORKER="1")
sys.path.insert(0,"."); import webhook as w
c=w.app.test_client(); A={"X-Admin-Key":"adminkey1"}
def send(lid):
    body=json.dumps({"object":"page","entry":[{"id":"P1","changes":[{"field":"leadgen","value":{"leadgen_id":lid,"page_id":"P1","form_id":"F1","ad_id":"AD9","adgroup_id":"AS9","created_time":1}}]}]}).encode()
    sig="sha256="+hmac.new(b"appsecret1",body,hashlib.sha256).hexdigest()
    return c.post("/webhook/facebook-leads",data=body,headers={"X-Hub-Signature-256":sig,"Content-Type":"application/json"})
def status(lid):
    import sqlite3; r=sqlite3.connect(d+"/t.db").execute("select status,attempts,last_error_class from leads where leadgen_id=?",(lid,)).fetchone(); return r
FAILS=[]
def chk(name, ok):
    print(("PASS " if ok else "FAIL ")+name)
    if not ok: FAILS.append(name)
chk("verify ok", c.get("/webhook/facebook-leads?hub.mode=subscribe&hub.verify_token=vt-123456&hub.challenge=77").data==b"77")
chk("verify bad token 403", c.get("/webhook/facebook-leads?hub.mode=subscribe&hub.verify_token=x&hub.challenge=1").status_code==403)
chk("bad signature 403", c.post("/webhook/facebook-leads",data=b"{}",headers={"X-Hub-Signature-256":"sha256=00"}).status_code==403)
send("L1"); send("L1"); w.run_once()
chk("success -> done", status("L1")[0]=="done")
p=state["last"]; chk("payload mapped", p["Name"]=="Asha K" and p["Phone"]=="+911234567890" and p["Email"]=="" and p["Ad ID"]=="AD9" and p["Adset ID"]=="AS9" and p["Lead Source"]=="FB Ads" and p["leadgen_id"]=="L1" and p["Disclaimer Consent"]=="checked" and "full_name" not in p)
state["erp"]="err200"; send("L2"); w.run_once()
chk("HTTP200 error body -> dead immediately", status("L2")[0]=="dead" and status("L2")[2]=="permanent")
state["erp"]="dup"; send("L3"); w.run_once(); chk("409 -> done", status("L3")[0]=="done")
state["erp"]="500"; send("L4"); w.run_once(); chk("500 -> pending transient", status("L4")==("pending",1,"transient"))
state["erp"]="ok"; state["graph"]="190"; send("L5"); w.run_once(); chk("graph 190 -> pending token", status("L5")==("pending",1,"token"))
import sqlite3; cn=sqlite3.connect(d+"/t.db"); cn.execute("update leads set next_attempt_at='2000-01-01T00:00:00Z' where leadgen_id='L4'"); cn.commit()
for i in range(5):
    state["erp"]="500"; w.run_once(); cn.execute("update leads set next_attempt_at='2000-01-01T00:00:00Z' where leadgen_id='L4'"); cn.commit()
chk("6th failure -> dead", status("L4")[0]=="dead" and status("L4")[1]==6)
chk("admin 401 w/o key", c.get("/admin/stats").status_code==401)
s=c.get("/admin/stats",headers=A).get_json(); chk("stats", s["counts"]["dead"]==2 and s["counts"]["done"]==2)
chk("dead list", c.get("/admin/dead-leads",headers=A).get_json()["total"]==2)
chk("dead detail", len(c.get("/admin/dead-leads/L2",headers=A).get_json()["attempts"])==1)
chk("requeue needs filter", c.post("/admin/requeue",headers=A,json={}).status_code==400)
state["erp"]="ok"; state["graph"]="ok"
r=c.post("/admin/requeue",headers=A,json={"leadgen_ids":["L2"],"refetch":True}).get_json(); w.run_once()
chk("requeue+refetch -> done", r["requeued"]==1 and status("L2")[0]=="done")
chk("no page token stored in DB", b"TOKEN_ABCDEFG" not in open(d+"/t.db","rb").read())
# --- testing.md behaviours
def raw(lid, page="P1"):
    body=json.dumps({"object":"page","entry":[{"id":page,"changes":[{"field":"leadgen","value":{"leadgen_id":lid,"page_id":page,"form_id":"F1"}}]}]}).encode()
    sig="sha256="+hmac.new(b"appsecret1",body,hashlib.sha256).hexdigest()
    return c.post("/webhook/facebook-leads",data=body,headers={"X-Hub-Signature-256":sig,"Content-Type":"application/json"}).get_json()
chk("queued 1", raw("N1")["queued"]==1)
chk("duplicates 1", raw("N1")["duplicates"]==1)
chk("unknown page skipped 1", raw("N2","000")["skipped"]==1)
state["graph"]="ok"; w.run_once()
orig = H.do_POST
def p2(self):
    n=int(self.headers["Content-Length"]); self.rfile.read(n)
    self._send(200,{"exception":"frappe.exceptions.ValidationError","_server_messages":json.dumps([json.dumps({"message":"Error: Value missing for Lead: Customer Name"})])})
H.do_POST=p2; raw("N3"); w.run_once()
cn=sqlite3.connect(d+"/t.db"); r=cn.execute("select status,last_error_class,last_error from leads where leadgen_id='N3'").fetchone()
chk("'exception' + _server_messages -> dead w/ message", r[0]=="dead" and "Customer Name" in r[2])
def p3(self):
    n=int(self.headers["Content-Length"]); self.rfile.read(n)
    self._send(200,{"exc_type":"QueryTimeoutError","_server_messages":json.dumps([json.dumps({"message":"Lock wait timeout exceeded"})])})
H.do_POST=p3; raw("N4"); w.run_once()
chk("lock timeout in 200 body -> retried", status("N4")==("pending",1,"transient"))

# --- mapping unit tests
cfg = w.get_config()
def B(fields, graph_extra=None, cfg=cfg):
    g = {"field_data":[{"name":k,"values":v if isinstance(v,list) else [v]} for k,v in fields.items()],"ad_name":"Test ad","adset_name":"Test Adset","campaign_name":"Test Campaign","platform":"ig"}
    g.update(graph_extra or {})
    return w.build_payload({"page_id":"P1","leadgen_id":"X","webhook_data":"{}"}, g, cfg)
p = B({"full_name":"  Test   Person ","phone_number":"p:+91 12345-67891","email":" Test.Person@Example.com ","city_name":"Ludhiana","What_is_your_budget?":["1L","2L"]})
chk("core mapping+normalise", (p["Name"],p["Phone"],p["Email"])==("Test Person","+911234567891","test.person@example.com"))
chk("ad names", (p["Ad Name"],p["Adset Name"],p["Campaign"])==("Test ad","Test Adset","Test Campaign"))
chk("unmapped answers pass through", p["City Name"]=="Ludhiana" and p["What Is Your Budget?"]=="1L, 2L")
chk("first+last name fallback", B({"first_name":"A","last_name":"B","email":"a@b.c"})["Name"]=="A B")
chk("collision keeps both", B({"platform":"x"})["Platform"]=="ig" and B({"platform":"x"})["Platform (form)"]=="x")
import copy
c2 = copy.deepcopy(cfg); c2["payload"]={"field_mapping":{"City":["city_name"],"Phone":"mobile_no"},"drop_fields":["junk"],"static_fields":{"Territory":"India"},"send_meta_fields":["ad_name","adset_name","campaign_name"],"meta_field_names":{"campaign_name":"Campaign Name"}}
p = B({"city_name":"Ldh","mobile_no":"9876543210","junk":"zz","other":"o"}, cfg=c2)
chk("custom mapping/drop/static/meta trim", p["City"]=="Ldh" and p["Phone"]=="9876543210" and "Junk" not in p and p["Territory"]=="India" and p["Campaign Name"]=="Test Campaign" and "Ad ID" not in p and p["Other"]=="o")
try: w.validate_payload_cfg({"send_meta_fields":["nope"]}); chk("bad config rejected", False)
except RuntimeError: chk("bad config rejected", True)
H.do_POST=orig
# required field -> dead before calling ERPNext, then fixed mapping applies on requeue WITHOUT refetch
state["erp"]="ok"; state["graph"]="ok"
c3 = json.load(open(d+"/c.json")); c3["payload"]={"required_fields":["City"]}; json.dump(c3, open(d+"/c.json","w")); os.utime(d+"/c.json",(1,time.time()+5) if False else None)
import time; time.sleep(0.05); os.utime(d+"/c.json",(time.time()+10,time.time()+10))
raw("R1"); w.run_once()
chk("required field missing -> dead (map stage)", status("R1")[0]=="dead")
c3["payload"]={"required_fields":["City"],"field_mapping":{"City":["full_name"]}}; json.dump(c3, open(d+"/c.json","w")); os.utime(d+"/c.json",(time.time()+20,time.time()+20))
c.post("/admin/requeue",headers=A,json={"leadgen_ids":["R1"]}); w.run_once()
chk("fixed mapping applies on plain requeue", status("R1")[0]=="done" and state["last"]["City"]=="Asha K")
print(f"\n{len(FAILS)} failed" if FAILS else "\nall checks passed")
sys.exit(1 if FAILS else 0)
