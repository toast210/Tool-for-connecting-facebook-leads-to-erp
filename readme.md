# Facebook Lead Ads → ERPNext Lead Pipeline

A small, reliable service (`webhook.py`) that receives Facebook Lead Ads webhooks for one or more Pages, queues each lead in SQLite, fetches the full lead from the Graph API, maps it into a flat JSON object and sends it to ERPNext. If anything fails it retries with exponential backoff, records every attempt, emails you when a lead gives up ("dead"), and exposes dead leads through an admin API so they can be inspected and requeued. Your website's forms can send leads into the same pipeline through `POST /webhook/website-leads` (section 17).

---

## Table of contents

1. [How it works](#1-how-it-works)
2. [Quick start](#2-quick-start)
3. [Configuration (`credentials.json`)](#3-configuration-credentialsjson)
4. [Multiple Pages and forms](#4-multiple-pages-and-forms)
5. [What is sent to ERPNext (field mapping)](#5-what-is-sent-to-erpnext-field-mapping)
6. [ERPNext side](#6-erpnext-side)
7. [Lead lifecycle, retries and error classes](#7-lead-lifecycle-retries-and-error-classes)
8. [Email alerts](#8-email-alerts)
9. [HTTP endpoints](#9-http-endpoints)
10. [Database](#10-database)
11. [Facebook setup](#11-facebook-setup)
12. [Deployment](#12-deployment)
13. [Security](#13-security)
14. [Operations runbook](#14-operations-runbook)
15. [Testing](#15-testing)
16. [Limits and known gaps](#16-limits-and-known-gaps)
17. [Website form leads](#17-website-form-leads)

---

## 1. How it works

```
 Facebook Lead Ads (any configured Page, any form)      Your website's server
        │  POST /webhook/facebook-leads                  │  POST /webhook/website-leads
        │  (leadgen_id, page_id, form_id, ...)           │  (the full submission)
        ▼                                                ▼
 ┌────────────────────────────────────────────────────────────┐
 │  Flask app (webhook.py) — ONE process                      │
 │                                                            │
 │  Facebook ─► verify X-Hub-Signature-256 ─► page configured?│
 │              ─► INSERT OR IGNORE into SQLite ─► 200        │
 │  Website  ─► check X-Website-Key ─► phone or email given?  │
 │              ─► INSERT OR IGNORE (with its data) ─► 200    │
 │                                                            │
 │  Worker thread(s), every few seconds:                      │
 │    1. claim a due lead (pending → processing)              │
 │    2. fetch:  Graph API (page token + appsecret_proof);    │
 │               skipped for website leads                    │
 │    3. map:    build the flat payload (section 5)           │
 │    4. push:   POST to erpnext.lead_url                     │
 │    5. done │ retry later │ dead (+ alert email)            │
 │                                                            │
 │  Admin API (X-Admin-Key): stats, dead leads, requeue,      │
 │                           test alert email                 │
 └────────────────────────────────────────────────────────────┘
        │                                   │
        ▼                                   ▼
   SQLite (WAL)                     ERPNext custom method / Lead API
```

Why a queue: the Facebook webhook only stores the lead ID and returns 200 in milliseconds. A website submission is stored with its form data, since there is no Graph API to fetch it from later. All slow or failure-prone work (Graph, ERPNext) happens in the background worker with retries, so a slow or down ERPNext never loses a lead and never makes Facebook resend the webhook. `leadgen_id` is unique in the database, so a webhook delivered twice is queued once.

The raw Graph response is stored after the first successful fetch. The ERPNext payload is **rebuilt from it on every attempt**, so a mapping fix in `credentials.json` takes effect on the next retry or requeue without contacting Facebook again.

---

## 2. Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

1. Create `credentials.json` next to `webhook.py` (section 3). It is git-ignored; never commit it.
2. Run the tests: `python smoke_test.py` (section 15).
3. Run the service:
   - Development: `python webhook.py` (listens on `0.0.0.0:3000`, or `PORT`).
   - Production: see section 12 (`gunicorn` on Linux, `waitress` on Windows).
4. Put it behind HTTPS and register `https://<your-domain>/webhook/facebook-leads` with Facebook (section 11).
5. Send yourself a test alert: `curl -X POST https://<your-domain>/admin/test-alert -H "X-Admin-Key: <admin_api_key>"`.

The module creates the database and starts the worker thread(s) as soon as it is imported, and refuses to start if `credentials.json` is missing or invalid.

---

## 3. Configuration (`credentials.json`)

Full example:

```json
{
  "verify_token": "long-random-string-for-the-facebook-handshake",
  "app_secret": "facebook-app-secret",
  "admin_api_key": "another-long-random-string-for-the-admin-api",
  "graph_api_version": "v26.0",

  "erpnext": {
    "lead_url": "ERP-url-for-adding-leads",
    "api_key": "erpnext-api-key",
    "api_secret": "erpnext-api-secret"
  },

  "pages": {
    "111111111111111": { "name": "Main Page",   "page_access_token": "EAAB...", "source": "Facebook - Main Page" },
    "222222222222222": { "name": "Second Page", "page_access_token": "EAAB...", "source": "Facebook - Second Page" },
    "333333333333333": { "name": "Third Page",  "page_access_token": "EAAB...", "source": "Facebook - Third Page" }
  },

  "alert_email": {
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "security": "starttls",
    "username": "you@company.com",
    "password": "app-password",
    "to": "you@company.com"
  }
}
```

### Required keys

| Key | Meaning |
|---|---|
| `verify_token` | Any secret string. Facebook sends it back during the webhook handshake. |
| `app_secret` | Facebook App secret (App settings → Basic). Used to verify `X-Hub-Signature-256` and to sign Graph calls (`appsecret_proof`). |
| `admin_api_key` | Secret for the `/admin/*` endpoints (`X-Admin-Key` header). Use a different value from `verify_token`. |
| `erpnext.lead_url` *or* `erpnext.base_url` | Where leads are POSTed. `lead_url` is used as-is (e.g. a custom `/api/method/...`). With only `base_url`, the URL is `base_url + /api/resource/Lead`. |
| `pages` | Object keyed by **Facebook Page ID** (as a string). At least one Page. |

### Per-Page keys (`pages.<page_id>`)

| Key | Default | Meaning |
|---|---|---|
| `page_access_token` | — (required to fetch leads) | Long-lived Page access token with `leads_retrieval`. |
| `name` | Page ID | Label used in logs and alert emails. |
| `source` | `Facebook Lead Ads` | Sent to ERPNext as `Lead Source`. |

### Optional keys

| Key | Default | Meaning |
|---|---|---|
| `graph_api_version` | `v26.0` | Graph API version in the fetch URL. |
| `graph_base_url` | `https://graph.facebook.com` | Only change for testing against a mock server. |
| `graph_lead_fields` | see below | Fields requested from the Graph Lead object (list or comma-separated string). |
| `erpnext.api_key` + `erpnext.api_secret` | none | Sends `Authorization: token key:secret`. Needed unless the ERPNext method allows guests. |
| `erpnext.extra_headers` | none | Extra HTTP headers for every ERPNext request (object). |
| `erpnext.leadgen_id_field` | `leadgen_id` | Payload key (and ERPNext field) that carries the Facebook lead ID. |
| `erpnext.duplicate_check` | `false` | Before POSTing, GET the Lead list filtered by `leadgen_id_field`; if found, mark `done`. |
| `erpnext.duplicate_check_url` | `lead_url` | URL for that lookup. **Set it to `https://<erp>/api/resource/Lead` when `lead_url` is a custom method.** |
| `payload` | see section 5 | Field-mapping options. |
| `alert_email` | none (no alerts) | SMTP settings for dead-lead emails (section 8). |
| `website` | none (endpoint off) | Turns on `POST /webhook/website-leads`: `api_key` (or `api_keys`), optional `source`, `name` and a website-only `field_mapping` (section 17). |
| `worker_threads` | `1` | Worker threads, 1 to 8. Read at startup only. |
| `max_requeues` | `5` | A dead lead is not requeued again after this many requeues. |
| `clear_payload_on_done` | `false` | Erase the stored Graph data and payload once a lead succeeds. |

Default `graph_lead_fields`: `id, created_time, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, form_id, field_data, is_organic, platform, custom_disclaimer_responses`. If Facebook rejects a field for your API version, leads fail at the `fetch` stage as `permanent`. Remove the field and requeue with `"refetch": true`.

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `CREDENTIALS_PATH` | `credentials.json` | Path to the config file. |
| `DATABASE_PATH` | `leads.db` | SQLite file. |
| `PORT` | `3000` | Port for `python webhook.py` only. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, ... |
| `DISABLE_WORKER` | unset | `1` = do not start worker threads (used by the tests). |

### Live reload

The file is re-read automatically whenever its modification time changes, so new Pages, tokens, mapping or email settings apply without a restart (except `worker_threads`). If you save an invalid file while the service is running, the error is logged and **the previous valid settings stay in use**.

---

## 4. Multiple Pages and forms

- **Pages:** add one entry per Page under `pages`. Each lead uses the token of the Page it came from, and the Page's `source` becomes its `Lead Source`. All Pages must be subscribed to the **same** Facebook App (there is one `app_secret`).
- **Forms:** every form on a configured Page is accepted; no per-form setup is needed. The `form_id` is sent as `Form ID`. Different forms can ask different questions: name/phone/email are matched by common question names, and every other answer is passed through (section 5).
- **Unconfigured Pages:** a lead whose Page ID is not in `pages` is **logged and dropped** (the webhook still returns 200, so Facebook will not resend it). Add a Page to `credentials.json` *before* subscribing it to the app.
- The `payload` mapping is shared by all Pages and forms; there are no per-form overrides.
- `/admin/stats` and the dead-lead filters work per Page.

---

## 5. What is sent to ERPNext (field mapping)

The worker POSTs one flat JSON object per lead. Example:

```json
{
  "Name": "Test Person",
  "Phone": "+911234567891",
  "Email": "test.person@example.com",
  "Ad Name": "Test ad",
  "Adset Name": "Test Adset",
  "Campaign": "Test Campaign",
  "City": "Ludhiana",
  "Disclaimer Consent": "checked",
  "Ad ID": "120200000000001",
  "Adset ID": "120200000000002",
  "Campaign ID": "120200000000003",
  "Form ID": "555",
  "Created Time": "2026-10-30T10:00:00+0000",
  "Platform": "fb",
  "Is Organic": false,
  "Page ID": "123456789",
  "Lead Source": "Facebook Lead Ads",
  "leadgen_id": "1234567890123"
}
```

How it is built, in order:

1. **Mapped answers.** Each output field takes the first matching form question (matched case-insensitively, ignoring spaces and punctuation). Defaults:

   | Output | Form question names tried |
   |---|---|
   | `Name` | `full_name`, `name`, `your_name`, `customer_name` (falls back to `first_name` + `last_name`) |
   | `Phone` | `phone_number`, `phone`, `mobile`, `mobile_number`, `work_phone_number`, `whatsapp_number` |
   | `Email` | `email`, `email_address`, `work_email` |

   A mapped field with no matching answer is sent as `""`. Multi-select answers are joined with `, `.
2. **Normalisation** (on by default): phone fields lose spaces, dashes and Meta's `p:` prefix (`p:+91 98765-43210` → `+919876543210`); emails are trimmed and lower-cased; `Name` has extra spaces collapsed.
3. `Ad Name`, `Adset Name`, `Campaign`.
4. **Every other answer**, under a readable name (`city_name` → `City Name`). If that name collides with another key, ` (form)` is appended.
5. **Disclaimer checkboxes** as `Disclaimer <Key>`.
6. Remaining metadata: `Ad ID`, `Adset ID`, `Campaign ID`, `Form ID`, `Created Time`, `Platform`, `Is Organic`, `Page ID`, `Lead Source`. If Graph does not return `ad_id`, `adset_id`, `form_id` or `created_time`, the values from the webhook notification are used. `Created Time` is passed through as Facebook sends it (`2026-10-30T10:00:00+0000`).
7. `static_fields`, then the lead ID under `erpnext.leadgen_id_field` (`leadgen_id`).

### `payload` options

```json
"payload": {
  "field_mapping": { "City": ["city", "city_name"], "Phone": "mobile_no" },
  "required_fields": ["Name", "Phone"],
  "drop_fields": ["internal_question"],
  "send_meta_fields": ["ad_name", "adset_name", "campaign_name", "form_id", "page_id", "lead_source"],
  "meta_field_names": { "campaign_name": "Campaign Name" },
  "include_unmapped_answers": true,
  "normalize_values": true,
  "phone_fields": ["Phone"],
  "email_fields": ["Email"]
}
```

| Option | Default | Meaning |
|---|---|---|
| `field_mapping` | defaults above | Output name → form question name or list of names. Adds to / replaces the defaults per output name. |
| `required_fields` | `[]` | If any of these output fields is empty, the lead goes **dead at stage `map`** without calling ERPNext. |
| `drop_fields` | `[]` | Form questions never sent as unmapped answers. |
| `static_fields` | `{}` | Constant key/values added to every payload (override earlier keys). |
| `send_meta_fields` | all | Which metadata to send. Allowed: `ad_name`, `adset_name`, `campaign_name`, `ad_id`, `adset_id`, `campaign_id`, `form_id`, `created_time`, `platform`, `is_organic`, `page_id`, `lead_source`. |
| `meta_field_names` | names above | Rename metadata output keys (same allowed keys). |
| `include_unmapped_answers` | `true` | Send answers that were not mapped. |
| `normalize_values` | `true` | Phone/email/name clean-up (step 2). |
| `phone_fields`, `email_fields` | `["Phone"]`, `["Email"]` | Which outputs get phone / email normalisation. |

Invalid `payload` settings are rejected when the file is loaded, with a message naming the problem.

---

## 6. ERPNext side

### 6.1 Receiving method

With `lead_url` pointing at a custom method (as in this project), that method receives the JSON in section 5 and creates the Lead. Requirements:

- **Authentication:** either `@frappe.whitelist(allow_guest=True)` or an API user whose `api_key`/`api_secret` are set in `credentials.json`. A 401/403 is classed `token` and retried, then the lead dies.
- **Accept any keys.** Keys contain spaces (`Ad Name`) and vary by form, so read the body as a dict rather than naming parameters.
- **Fail loudly.** If the Lead cannot be created, `frappe.throw(...)` (or return an HTTP 4xx/5xx). The service also detects errors returned inside a 200 response (`exc`, `exc_type`, `exception`, `error`, `status: "error"/"failed"`, `success: false`), but a method that swallows the error and returns normally makes the lead count as delivered.
- **Be idempotent.** If a Lead with this `leadgen_id` already exists, return it instead of creating another (a retry after a lost response would otherwise duplicate it). Errors mentioning "duplicate" plus the `leadgen_id` field, `exc_type` `Duplicate...`, or HTTP 409 are treated as success.
- **Return the new Lead's name** (`{"message": {"name": "CRM-LEAD-..."}}`); it is written to the service log.

Sketch:

```python
import frappe

@frappe.whitelist()
def socialmediaform_webhook_listener(**kwargs):
    data = frappe.request.get_json(silent=True) or dict(frappe.form_dict)
    lid = data.get("leadgen_id")
    existing = lid and frappe.db.get_value("Lead", {"leadgen_id": lid})
    if existing:
        return {"name": existing, "duplicate": True}
    lead = frappe.get_doc({
        "doctype": "Lead",
        "lead_name": data.get("Name") or "Facebook Lead",
        "mobile_no": data.get("Phone"),
        "email_id": data.get("Email"),
        "source": data.get("Lead Source"),
        "leadgen_id": lid,
        # map any other keys you need: data.get("Campaign"), data.get("City"), ...
    })
    lead.insert(ignore_permissions=True)
    return {"name": lead.name}
```

If you POST straight to `/api/resource/Lead` instead (`base_url` only), the payload keys must be real Lead fieldnames, so set `payload.field_mapping`, `meta_field_names` and `static_fields` to produce `lead_name`, `mobile_no`, `email_id`, etc., and set `include_unmapped_answers` to `false`.

### 6.2 Custom field

Add a custom field on **Lead**: label `Leadgen ID`, fieldname `leadgen_id`, type Data, **Unique**. It is what makes retries and requeues safe, and it is required for `erpnext.duplicate_check`.

### 6.3 Optional dead-lead report in ERPNext

Dead leads are emailed (section 8) and available through the admin API (section 9). To show them inside ERPNext, call the admin API from whitelisted **server-side** methods (the admin key must never reach the browser):

```python
import frappe, requests

def _svc():
    return frappe.conf.get("lead_service_url"), frappe.conf.get("lead_service_admin_key")

@frappe.whitelist()
def get_dead_leads(page_id=None, limit=50, offset=0):
    frappe.only_for(["System Manager", "Sales Manager"])
    base, key = _svc()
    params = {"limit": limit, "offset": offset, **({"page_id": page_id} if page_id else {})}
    r = requests.get(f"{base}/admin/dead-leads", params=params, headers={"X-Admin-Key": key}, timeout=15)
    r.raise_for_status()
    return r.json()

@frappe.whitelist()
def requeue(leadgen_ids):
    frappe.only_for(["System Manager", "Sales Manager"])
    base, key = _svc()
    r = requests.post(f"{base}/admin/requeue", json={"leadgen_ids": frappe.parse_json(leadgen_ids)},
                      headers={"X-Admin-Key": key}, timeout=15)
    r.raise_for_status()
    return r.json()
```

Put `lead_service_url` and `lead_service_admin_key` in `site_config.json`.

---

## 7. Lead lifecycle, retries and error classes

### Statuses

| Status | Meaning |
|---|---|
| `pending` | Waiting for `next_attempt_at`. New leads are due immediately. |
| `processing` | Claimed by a worker. Rows stuck here for more than 5 minutes (crash, restart) are reset to `pending` automatically. |
| `done` | Delivered to ERPNext (or ERPNext reported it already exists). |
| `dead` | Gave up: a `permanent` error, or still failing after 5 retries. An alert email is sent. |

```
 webhook ─► pending ──claim──► processing ──success──► done
              ▲  ▲                 │
              │  └─ transient/token ┤ (wait, retry)
              │                     ▼ permanent, or 6th failure
              └──── requeue ───── dead ──► alert email
```

### Retry schedule

Up to **5 retries** (6 attempts). Waits double each time, with ±10% random jitter:

| Failure | Next attempt after | Time since first failure |
|---|---|---|
| 1 | ~1 min | ~1 min |
| 2 | ~2 min | ~3 min |
| 3 | ~4 min | ~7 min |
| 4 | ~8 min | ~15 min |
| 5 | ~16 min | ~31 min |
| 6 | — lead becomes `dead` | |

If ERPNext or Facebook is down for longer than ~31 minutes, leads that arrived during the outage go dead. That is recoverable: Facebook keeps leads for about 90 days, and a requeue after the outage processes them normally.

### Stages

| Stage | What happens |
|---|---|
| `fetch` | Graph API call for the lead. |
| `map` | Building the payload; `required_fields` check. |
| `push` | Optional duplicate check, then the POST to ERPNext. |

### Error classes

| Class | Graph API | ERPNext | Behaviour |
|---|---|---|---|
| `transient` | Network errors, timeouts, HTTP 408/429/5xx, codes 1, 2, 4, 17, 32, 341, 613, 80000–80014, `is_transient` | Network errors, timeouts, HTTP 408/429/5xx, error text in a 200 body mentioning lock wait / deadlock / timeout / try again | Retried. |
| `token` | OAuth codes 10, 102, 190, 200–299 | HTTP 401, 403 | Retried (fix the token or ERPNext credentials). |
| `permanent` | Any other 4xx (e.g. lead deleted, unknown field) | Any other 4xx, or an error inside a 200 body | **Dead immediately.** |

Unexpected Python errors are treated as `transient` (still bounded by the retry limit). A "duplicate" response from ERPNext is success, not an error.

---

## 8. Email alerts

When a lead becomes **dead**, the service emails you. Nothing is sent for failures that will still be retried, so one dead lead = one email.

Configure under `alert_email` in `credentials.json`:

| Key | Default | Meaning |
|---|---|---|
| `smtp_host` | — (required) | SMTP server. |
| `smtp_port` | 587 (`starttls`), 465 (`ssl`), 25 (`none`) | SMTP port. |
| `security` | `starttls` | `starttls`, `ssl` (implicit TLS) or `none`. Certificates are verified. |
| `username`, `password` | none | SMTP login (set both or neither). |
| `from` | `username` | Sender address. |
| `to` | — (required) | One address or a list. |
| `subject_prefix` | `[FB Leads]` | Prepended to every subject. |

Common providers:

| Provider | `smtp_host` | Port / `security` | Password |
|---|---|---|---|
| Gmail / Google Workspace | `smtp.gmail.com` | 587 / `starttls` | A 16-character **App Password** (requires 2-Step Verification), not your normal password. |
| Microsoft 365 / Outlook | `smtp.office365.com` | 587 / `starttls` | Account password; SMTP AUTH must be enabled for the mailbox. |
| Zoho Mail | `smtp.zoho.in` (India) | 465 / `ssl` | Account or app-specific password. |

Each email contains: lead ID, Page name and ID, form ID, received and failure times (UTC), attempts, stage, error class, HTTP status, the error text (secrets removed), the lead's **Name / Phone / Email** when they were fetched (so you can follow up by hand), what to do for that error class, and a ready-to-use requeue command. Example subject:

```
[FB Leads] Lead 1234567890123 failed - Main Page [permanent/push]
```

**Test it:** `POST /admin/test-alert` sends a test email and returns `{"sent": true, "to": ...}`, or `400` with the SMTP error (e.g. `SMTPAuthenticationError ... not accepted` for a wrong password).

Emails are sent in a background thread; if sending fails, a warning is logged and the lead stays dead and visible in `/admin/dead-leads`. The `alert_email.password` is redacted from logs and stored errors. The old `alert_webhook_url` (chat webhook) setting is no longer used; a warning is logged if it is still present.

---

## 9. HTTP endpoints

### Public

| Method & path | Purpose |
|---|---|
| `GET /webhook/facebook-leads` | Facebook handshake. Returns `hub.challenge` if `hub.mode=subscribe` and `hub.verify_token` matches; otherwise 403. |
| `POST /webhook/facebook-leads` | Lead notifications. 403 on a bad/missing signature, 400 on invalid JSON, 500 if the database write fails (Facebook then retries), otherwise `200 {"status":"ok","queued":n,"duplicates":n,"skipped":n}`. Only `leadgen` changes are processed. |
| `POST /webhook/website-leads` | One website form submission (section 17). Needs `X-Website-Key` (or `Authorization: Bearer`). 404 if the `website` section is missing, 401 on a wrong key, 400 on a bad body or no phone/email, otherwise `200 {"status":"ok","lead_id":"web-…","duplicate":false}`. |
| `GET /health` | `{"status":"ok"}` if the database is reachable, else 503. Does not check the worker (use `/admin/stats`). |

Request bodies are limited to 1 MB.

### Admin (header `X-Admin-Key: <admin_api_key>`, otherwise 401)

**`GET /admin/stats`**

```json
{
  "counts": {"pending": 0, "processing": 0, "done": 42, "dead": 1},
  "oldest_unfinished_age_seconds": 0,
  "pages": {"123456789": {"last_success": "2026-11-03T05:40:12Z", "failures_last_hour": 0}},
  "workers_alive": 1
}
```

`workers_alive` counts worker threads seen in the last 30 s; `0` means the worker is not running.

**`POST /admin/test-alert`** — sends a test email (section 8).

**`GET /admin/dead-leads`** — query parameters (all optional): `page_id`, `since`, `until` (compared with `dead_at`, format `2026-11-03T00:00:00Z`), `limit` (default 50, max 500), `offset`.

```json
{
  "total": 1, "limit": 50, "offset": 0,
  "leads": [{
    "leadgen_id": "1234567890123", "page_id": "123456789", "form_id": "555",
    "attempts": 1, "requeues": 0,
    "last_error": "ERPNext rejected the lead (HTTP 200): ... Customer Name",
    "last_error_class": "permanent", "last_error_stage": "push",
    "created_at": "2026-11-03T05:40:00Z", "dead_at": "2026-11-03T05:40:03Z"
  }]
}
```

**`GET /admin/dead-leads/<leadgen_id>`** — `{"lead": {...}, "attempts": [...]}`. `lead` holds every column of the lead row, with `payload` (what was/would be sent), `graph_data` (raw Graph response) and `webhook_data` decoded as JSON. `attempts` is the full history (`attempt_no`, `stage`, `ok`, `error_class`, `http_status`, `response_body`, `error`, `created_at`). 404 if the lead is not dead.

**`POST /admin/requeue`** — moves **dead** leads back to `pending`, due now, with `attempts = 0` and `requeues + 1`. History is kept. One filter is required:

```json
{ "leadgen_ids": ["1234567890123"] }
{ "page_id": "123456789", "since": "2026-11-03T00:00:00Z", "until": "2026-11-03T12:00:00Z" }
{ "all": true }
```

Add `"refetch": true` to discard the stored Graph data so the lead is fetched from Facebook again (after changing `graph_lead_fields` or fixing a permission). Without it, the stored Graph data is reused and the payload is rebuilt with the current mapping. Leads already requeued `max_requeues` times are skipped.

```json
{ "requeued": 1, "skipped_over_max_requeues": [], "leadgen_ids": ["1234567890123"] }
```

---

## 10. Database

SQLite in WAL mode (`busy_timeout` 30 s), one connection per operation. Created automatically at `DATABASE_PATH`.

| Table | Contents |
|---|---|
| `leads` | One row per lead: `leadgen_id` (unique), `page_id`, `form_id`, `status`, `attempts`, `requeues`, `next_attempt_at`, `locked_at`, `last_error`, `last_error_class`, `last_error_stage`, `payload` (JSON sent to ERPNext), `graph_data` (raw Graph JSON; for website leads, the submitted form fields), `webhook_data` (the notification's `value`; for website leads, `form_id`, `created_time` and the sender's `ip`), `created_at`, `updated_at`, `done_at`, `dead_at`. |
| `lead_attempts` | Every attempt: `leadgen_id`, `attempt_no`, `stage`, `ok`, `error_class`, `http_status`, `response_body` (≤ 2 KB, redacted), `error`, `created_at`. |
| `workers` | Worker heartbeats for `/admin/stats`. |

All timestamps are UTC, format `YYYY-MM-DDTHH:MM:SSZ`.

Maintenance runs every 60 s: stale `processing` rows (> 5 min) go back to `pending`; `done` leads older than **30 days** and `dead` leads older than **90 days** are deleted with their attempts.

The database holds personal data (names, phones, emails in `payload`/`graph_data`, website senders' IP addresses in `webhook_data`, and error bodies that may echo them). The IP is taken from the first `X-Forwarded-For` entry, which the caller can set, so treat it as a hint only. With the nginx config in section 12 that header is not set, so the IP is usually `127.0.0.1`. Add `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;` to record real addresses. Restrict access to the file and its backups. `clear_payload_on_done` removes the data from successful leads immediately. Losing the database is recoverable for about 90 days, since leads can be refetched from Facebook.

---

## 11. Facebook setup

1. Create a Facebook App (Business type) and add the **Webhooks** product.
2. Under Webhooks choose the **Page** object, set the callback URL to `https://<your-domain>/webhook/facebook-leads` and the verify token to your `verify_token`. Facebook calls the GET route to verify it.
3. Subscribe to the **leadgen** field.
4. **Add each Page to `pages` in `credentials.json` first**, then subscribe it to the app (Page subscription for `leadgen`).
5. For each Page, get a **long-lived Page access token** (from a long-lived user token or a system user) with `leads_retrieval`, `pages_manage_metadata`, `pages_show_list` and `pages_read_engagement` as required. For ad-level fields (`ad_*`, `adset_*`, `campaign_*`) the token's user also needs `ads_management` and advertiser access to the ad account.
6. Copy the **App secret** into `app_secret`.
7. Test with Facebook's **Lead Ads Testing Tool**, which sends a real webhook for a test lead.

Permissions and app-review rules change; check Meta's documentation for your `graph_api_version`.

---

## 12. Deployment

Run **exactly one process** with several threads. Each process starts its own worker thread(s), and SQLite prefers a single writer. Do not use `python webhook.py` in production.

### Windows (waitress)

```powershell
.venv\Scripts\waitress-serve --host 127.0.0.1 --port 3000 --threads 4 webhook:app
```

Run it as a service (for example with NSSM or Task Scheduler "at startup") with the working directory set to the project folder, or set `CREDENTIALS_PATH` and `DATABASE_PATH`.

### Linux (gunicorn + systemd)

```bash
gunicorn --workers 1 --threads 4 --bind 127.0.0.1:3000 webhook:app
```

Do not use `--preload` (worker threads do not survive the fork).

```ini
[Unit]
Description=Facebook Leads to ERPNext
After=network.target

[Service]
User=leadsvc
WorkingDirectory=/opt/fb-leads
Environment=CREDENTIALS_PATH=/opt/fb-leads/credentials.json
Environment=DATABASE_PATH=/var/lib/fb-leads/leads.db
ExecStart=/opt/fb-leads/.venv/bin/gunicorn --workers 1 --threads 4 --bind 127.0.0.1:3000 webhook:app
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

### HTTPS (nginx)

Facebook requires HTTPS, and the admin API returns personal data.

```nginx
server {
    listen 443 ssl;
    server_name leads.example.com;
    # ssl_certificate / ssl_certificate_key ...

    location /webhook/ { proxy_pass http://127.0.0.1:3000; proxy_set_header Host $host; }
    location /health   { proxy_pass http://127.0.0.1:3000; }
    location /admin/ {
        allow 203.0.113.10;   # your ERPNext server / office IP
        deny all;
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
    }
}
```

On restart nothing is lost: leads are in SQLite and any `processing` row is reset to `pending`.

---

## 13. Security

| Area | Measure |
|---|---|
| Webhook authenticity | `X-Hub-Signature-256` HMAC over the raw body with `app_secret`, constant-time compare. |
| Handshake | `verify_token` compared in constant time. |
| Graph calls | `appsecret_proof` sent with every request. |
| Admin API | `X-Admin-Key`, constant-time compare. Serve over HTTPS and IP-restrict `/admin/`. |
| Website endpoint | `X-Website-Key` compared in constant time against each configured key; send it from the website's **server** only, never from browser JavaScript. Keys rotate by listing old and new in `api_keys`. |
| Requeue abuse | A filter is required, `all` must be explicit, `max_requeues` caps loops. |
| Secrets | `credentials.json` is git-ignored; keep it readable only by the service user. |
| Redaction | Page tokens, app secret, admin key, verify token, ERPNext key/secret/extra headers, website API keys and the SMTP password are stripped from logs, stored errors and alert emails, as are `access_token=`, `appsecret_proof=` and `token ...` patterns. |
| Personal data | Stored in the database and included in alert emails (name/phone/email). Restrict the database file and the alert mailbox; use `clear_payload_on_done` and the retention purge. |
| ERPNext account | Use a dedicated API user that can only create (and read) Leads. |

---

## 14. Operations runbook

**Normal day:** nothing to do. Check `GET /admin/stats` occasionally: `workers_alive` ≥ 1 and `dead` not growing.

**You got an alert email.** The email states the class and stage:

| Class / stage | Likely cause | Action |
|---|---|---|
| `token` / `fetch`, several leads from one Page | Page token expired or revoked | Put a new token in `credentials.json` (reloads automatically), then requeue that Page: `{"page_id": "..."}`. |
| `token` / `push` | ERPNext API key/secret wrong or user disabled | Fix `erpnext.api_key`/`api_secret`, requeue. |
| `permanent` / `push` | ERPNext rejected the data (mandatory field, validation) | Fix the ERPNext method or the `payload` mapping, then requeue (no `refetch` needed). |
| `permanent` / `map` | A `required_fields` value was empty | Adjust `field_mapping` or `required_fields`, requeue. |
| `permanent` / `fetch` | Lead deleted, form access removed, or an invalid `graph_lead_fields` entry | Fix the field list and requeue with `refetch`; deleted leads cannot be recovered — use the contact details in the email. |
| `transient`, many at once | Facebook or ERPNext down > ~31 min | Once healthy, requeue the time range: `{"since": "...", "until": "..."}`. |

**No alert emails arriving:** call `POST /admin/test-alert` and read the returned error; check the service log for `alert email ... failed`.

**Leads not arriving at all:** check the log for `bad signature` (wrong `app_secret`) or `page ... is not configured` (Page missing from `pages`), and that the Page is subscribed to the app.

**Website leads not arriving:** the website gets the reason in the response — `401` wrong key, `400` no phone/email (the response lists the accepted field names), `404` no `website` section. The service log shows `Website: lead web-… queued` for each accepted one.

**Suspected duplicates in ERPNext:** make `leadgen_id` unique on Lead and make the receiving method return the existing Lead (section 6.1), or enable `erpnext.duplicate_check` with `duplicate_check_url`.

---

## 15. Testing

```bash
python smoke_test.py      # core behaviour; exits 1 if any check fails
pip install aiosmtpd
python email_test.py      # alert emails against a local SMTP server; exits 1 on failure
python website_test.py    # the website endpoint; exits 1 on failure
```

All three run entirely locally with mock Graph and ERPNext servers and a temporary database; they do not touch `credentials.json` or `leads.db`.

`smoke_test.py` covers: handshake, signature rejection, queueing and duplicate webhooks, unknown Pages, payload mapping and normalisation, custom mapping options, errors hidden in 200 responses, duplicate responses, transient/token retries, death after the 6th failure, the admin API, requeue (with and without refetch), `required_fields`, config validation, and that tokens are not stored.

`email_test.py` covers: the test-alert endpoint, one email per dead lead (and none for retries), email contents, secret redaction, wrong password, unreachable SMTP server, missing `alert_email`, config validation and multiple recipients.

`website_test.py` covers: endpoint off without config, key checks (header, Bearer, rotation), JSON and HTML-form posts, mapping and pass-through fields, Lead Source and metadata, no Graph call, duplicate `submission_id`, validation (no phone/email, bad JSON, too many fields, > 1 MB), custom `field_mapping`, retries, dead leads and their alert text, requeue with `refetch`, stats, config validation and key redaction.

Manual end-to-end checks after deployment:

1. Handshake: `curl "https://<host>/webhook/facebook-leads?hub.mode=subscribe&hub.verify_token=<token>&hub.challenge=123"` returns `123`.
2. `POST /admin/test-alert` → an email arrives.
3. Lead Ads Testing Tool → the lead appears in ERPNext with `leadgen_id`, and `/admin/stats` shows it `done`.
4. Temporarily break the ERPNext mapping → the next test lead goes dead and you get an email; fix it and requeue.

---

## 16. Limits and known gaps

| Aspect | Value |
|---|---|
| Webhook rate | Thousands per minute (one DB insert each). |
| Worker throughput | ~30–70 leads/min per worker thread (2–3 HTTP calls per lead); raise `worker_threads` (max 8). |
| Retry window | ~31 minutes, then dead (requeue to recover). |
| Facebook lead retention | ~90 days. |
| HTTP timeouts | 15 s for Graph and ERPNext, 20 s for SMTP. |
| Retention | `done` 30 days, `dead` 90 days. |

Not handled today:

- Leads from Pages not listed in `pages` are dropped, not stored.
- One `payload` mapping for all Pages and forms; no per-form settings or form allow-list.
- Alerts only on dead leads; an expired token is not reported until the first lead dies (~31 min).
- `/health` does not report a stopped worker (`/admin/stats` → `workers_alive` does).
- Retry count, backoff and retention periods are constants in `webhook.py`, not settings.
- Any HTTP 409 or `Duplicate...` error from ERPNext counts as "already exists", even when a field other than `leadgen_id` caused it. Such a lead is marked `done` without being created, and no alert is sent. Keep `leadgen_id` the only unique field your receiving method can clash on.
- Website lead ids are `web-<submission_id>`, without the form name. If two forms number their submissions separately, or the website's numbering restarts, a new lead with an id already in the database (done leads are kept 30 days) is answered `"duplicate": true` and not delivered. Send ids that are unique across all forms, such as UUIDs. A form field named `id`, `lead_id` or `submission_id` is used as the id, not sent as an answer.
- `erpnext.duplicate_check` without `duplicate_check_url` sends its lookup to `lead_url`. If that is a method that creates Leads, every lookup creates an empty Lead.
- Alert emails for website leads use the same `subject_prefix` (`[FB Leads]` by default).

---

## 17. Website form leads

Your website can send its form submissions to `POST /webhook/website-leads`. Each one joins the same queue as Facebook leads and goes to the same ERPNext method, with the same retries, dead-lead emails, admin API and requeue. There is no Graph call: the submission already carries the data.

### Turn it on

Generate a key (`python3 -c "import secrets; print(secrets.token_urlsafe(32))"`) and add to `credentials.json`:

```json
"website": {
  "api_key": "<the key>",
  "source": "Website",
  "name": "Website"
}
```

| Key | Default | Meaning |
|---|---|---|
| `api_key` | — (required) | Secret the website sends in `X-Website-Key`. At least 16 characters. |
| `api_keys` | — | A list instead of `api_key`, to rotate: add the new key, update the website, then remove the old one. |
| `source` | `Website` | Sent to ERPNext as `Lead Source`. |
| `name` | `Website` | Label in logs and alert emails. |
| `field_mapping` | none | Mapping for website leads only, applied on top of the defaults and `payload.field_mapping` (same format: ERPNext name → form field name or list of names). Facebook leads never see it. |

Example for a contact form with the fields `name`, `mobile`, `email`, `city`, `role`, `projectType`, `interest` (name, mobile and email are already covered by the defaults):

```json
"website": {
  "api_key": "<the key>",
  "source": "Website",
  "field_mapping": {
    "City": ["city"],
    "Profession": ["role"],
    "Project Type": ["projectType"],
    "Interested In": ["interest"]
  }
}
```

Every name in `field_mapping` is always sent for website leads (empty when the form left it blank), so ERPNext can rely on the key being there.

The file reloads by itself, so no restart is needed. Without a `website` section the endpoint answers 404. nginx needs no change: `/webhook/` is already forwarded.

### What the website sends

The website's **server** POSTs one submission per request. Never send from browser JavaScript, where the key would be visible to anyone.

```
POST https://<your-domain>/webhook/website-leads
X-Website-Key: <the key>
Content-Type: application/json

{
  "submission_id": "8f3c2a71-0b4e-4c1d-9a55-2f6e1d7c9b10",
  "form_name": "contact-us",
  "name": "Asha Kaur",
  "phone": "+91 98765 43210",
  "email": "asha@example.com",
  "city": "Ludhiana",
  "message": "Need a quote for 20 units"
}
```

- **Any field names work.** Name, phone and email are matched by the same names as Facebook forms (`name`/`full_name`/`first_name`+`last_name`, `phone`/`mobile`/`phone_number`/`whatsapp_number`, `email`/`email_address`, …) and by `payload.field_mapping` and `website.field_mapping`. Every other field is passed through under a readable name (`city` → `City`; camelCase is not split, so `projectType` → `ProjectType` unless you map it).
- **A phone or an email is required**; without one the answer is 400 and lists the accepted field names.
- **`submission_id`** (or `lead_id` / `id`): optional but recommended. A repeat with the same id is answered `"duplicate": true` and not sent to ERPNext twice, so the website can safely retry. Without one, every request becomes a new lead.
- **`form_name`** (or `form_id` / `form`): optional, sent as `Form ID`, so you can tell forms apart in ERPNext.
- A normal HTML form post (`application/x-www-form-urlencoded`) is accepted too. Lists are joined with `, `; `true`/`false` become `Yes`/`No`. Limits: 100 fields, 5,000 characters a value, 1 MB a request.

| Answer | Meaning |
|---|---|
| `200 {"status":"ok","lead_id":"web-…","duplicate":false}` | Queued; ERPNext gets it within seconds. |
| `400` | Not a JSON object or form post, no phone/email, or too many fields. Do not retry unchanged. |
| `401` | Missing or wrong key. |
| `404` | `website` section missing in `credentials.json`. |
| `413` | Body over 1 MB. |
| `500` | Database error: retry later with the same `submission_id`. |

### What ERPNext receives

The same flat JSON as Facebook leads (section 5), without the ad and campaign fields:

```json
{
  "Name": "Asha Kaur",
  "Phone": "+919876543210",
  "Email": "asha@example.com",
  "City": "Ludhiana",
  "Message": "Need a quote for 20 units",
  "Form ID": "contact-us",
  "Created Time": "2026-11-23T05:22:33Z",
  "Platform": "website",
  "Page ID": "website",
  "Lead Source": "Website",
  "leadgen_id": "web-8f3c2a71-0b4e-4c1d-9a55-2f6e1d7c9b10"
}
```

Website lead ids start with `web-`. In `/admin/stats`, dead-lead filters and requeue, website leads appear under the page id `website` (`{"page_id": "website"}` requeues them). A requeue with `refetch` keeps their data, since it is the only copy.

### Sending code

**PHP** (e.g. in the form's handler, after validating the input):

```php
<?php
$payload = [
    'submission_id' => bin2hex(random_bytes(16)),   // make it once per submission; reuse it if you retry
    'form_name'     => 'contact-us',
    'name'          => $_POST['name'] ?? '',
    'phone'         => $_POST['phone'] ?? '',
    'email'         => $_POST['email'] ?? '',
    'message'       => $_POST['message'] ?? '',
];
$ch = curl_init('https://<your-domain>/webhook/website-leads');
curl_setopt_array($ch, [
    CURLOPT_POST           => true,
    CURLOPT_HTTPHEADER     => ['Content-Type: application/json', 'X-Website-Key: ' . getenv('LEADS_WEBSITE_KEY')],
    CURLOPT_POSTFIELDS     => json_encode($payload),
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_TIMEOUT        => 10,
]);
$response = curl_exec($ch);
$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
if ($status !== 200) {
    error_log("lead not sent ($status): $response");   // keep the submission and retry later
}
```

**Node.js** (18+, server side):

```js
const res = await fetch("https://<your-domain>/webhook/website-leads", {
  method: "POST",
  headers: { "Content-Type": "application/json", "X-Website-Key": process.env.LEADS_WEBSITE_KEY },
  body: JSON.stringify({ submission_id: crypto.randomUUID(), form_name: "contact-us", name, phone, email, message }),
});
if (!res.ok) console.error("lead not sent", res.status, await res.text());
```

**Test from a terminal:**

```bash
curl -s -X POST https://<your-domain>/webhook/website-leads \
  -H "X-Website-Key: <the key>" -H "Content-Type: application/json" \
  -d '{"submission_id":"test-1","name":"Test Person","phone":"+911234567891","email":"test@example.com"}'
```

Keep the key in the website's server settings or environment (`LEADS_WEBSITE_KEY` above), not in its code repository.
