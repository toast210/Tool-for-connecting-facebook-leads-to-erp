# Facebook Lead Ads → ERPNext Lead Pipeline

A small, reliable service that receives Facebook Lead Ads webhooks, safely queues them, fetches the full lead from the Graph API, and pushes it into ERPNext. If anything fails, it retries automatically with exponential backoff, records every error, and exposes the leads that gave up ("dead" leads) to ERPNext so staff can see what went wrong and requeue them.

This document describes the complete design and how to implement, deploy and operate it. It is written so that someone who has not seen the original discussion can build it from scratch.

---

## Table of contents

1. [Why this design](#1-why-this-design)
2. [Architecture overview](#2-architecture-overview)
3. [Lead lifecycle and states](#3-lead-lifecycle-and-states)
4. [Retry policy](#4-retry-policy)
5. [Error classification](#5-error-classification)
6. [Database design (SQLite)](#6-database-design-sqlite)
7. [The webhook](#7-the-webhook)
8. [The worker thread](#8-the-worker-thread)
9. [Duplicate protection (idempotency)](#9-duplicate-protection-idempotency)
10. [Admin API (for ERPNext)](#10-admin-api-for-erpnext)
11. [ERPNext side](#11-erpnext-side)
12. [Configuration (`credentials.json`)](#12-configuration-credentialsjson)
13. [Facebook setup](#13-facebook-setup)
14. [Deployment](#14-deployment)
15. [Security](#15-security)
16. [Operations runbook](#16-operations-runbook)
17. [Capacity and limits](#17-capacity-and-limits)
18. [Testing checklist](#18-testing-checklist)
19. [Implementation checklist](#19-implementation-checklist)
20. [Future scaling](#20-future-scaling)

---

## 1. Why this design

The simple version of this service does everything inside the webhook request: receive the notification, call the Graph API, call ERPNext, and return 200. It works, but it has three weaknesses:

| Problem | What happens |
|---|---|
| **Lost leads** | If ERPNext is down or rejects the lead, the webhook still returns 200. Facebook never retries, so the lead exists only in a log line. |
| **Duplicates** | If ERPNext is slow, the request can outlast Facebook's response window. Facebook resends the webhook and the same lead can be created twice. |
| **No visibility** | Nobody can tell which leads failed or why without reading server logs. |

The fix is the **inbox (queue) pattern**:

1. The webhook only stores the lead's ID and returns 200 immediately.
2. A background worker does the slow work (Graph fetch, ERPNext push) and retries on failure.
3. The ID is the primary key, so repeated webhooks cannot create duplicate queue entries.
4. Every failure is recorded, and leads that exhaust their retries are exposed to ERPNext.

The webhook stores only the `leadgen_id`. The worker fetches the full lead from Facebook (which keeps it retrievable for about 90 days) and then stores the exact payload it sends to ERPNext on the lead row, so retries do not need Facebook again and you can always see what was sent.

---

## 2. Architecture overview

```
 Facebook Lead Ads
        │  POST /webhook/facebook-leads   (leadgen_id, page_id)
        ▼
 ┌──────────────────────────────────────────────────────┐
 │  Flask app (1 gunicorn process, a few threads)       │
 │                                                      │
 │  Webhook ──► verify signature ──► INSERT OR IGNORE   │
 │                                   into SQLite ──► 200│
 │                                                      │
 │  Worker thread (loop every few seconds)              │
 │    1. claim due lead (status=pending)                │
 │    2. fetch lead from Graph API                      │
 │    3. check ERPNext for existing leadgen_id          │
 │    4. POST lead to ERPNext                           │
 │    5. success → done │ failure → retry or dead       │
 │                                                      │
 │  Admin API (API key) ◄─────────── ERPNext            │
 │    GET  /admin/dead-leads                            │
 │    GET  /admin/dead-leads/<id>                       │
 │    POST /admin/requeue                               │
 └──────────────────────────────────────────────────────┘
        │                         ▲
        ▼                         │
   SQLite (WAL mode)       ERPNext (Lead doctype, report, requeue button)
```

Components:

- **Webhook**: accepts Facebook notifications, verifies the signature, queues IDs. No external calls.
- **Worker**: a single background thread that processes the queue with retries.
- **SQLite database**: the queue, the state of each lead, and the history of every attempt.
- **Admin API**: read-only view of dead leads for ERPNext, plus a requeue action.
- **ERPNext**: receives the leads, holds a `leadgen_id` custom field for duplicate protection, and shows an on-demand report of dead leads with a requeue button.

**Field mapping is done on the ERPNext side.** The service forwards the Facebook fields as they come (flattened to a simple key/value object) plus `lead_source` and `leadgen_id`. There is no `field_map` in the service configuration.

---

## 3. Lead lifecycle and states

Each lead row has one of four statuses:

| Status | Meaning |
|---|---|
| `pending` | Waiting for its `next_attempt_at` time. New leads start here, due immediately. |
| `processing` | Claimed by the worker and currently being handled. |
| `done` | Successfully created in ERPNext. |
| `dead` | Gave up (retries exhausted or a permanent error). Visible to ERPNext. |

Transitions:

```
          webhook
             │
             ▼
         ┌─────────┐   claim    ┌────────────┐   success   ┌──────┐
         │ pending │ ─────────► │ processing │ ──────────► │ done │
         └─────────┘            └────────────┘             └──────┘
            ▲   ▲                    │
            │   │   transient error  │ permanent error, or
            │   └────────────────────┤ retries exhausted
            │     (schedule retry)   ▼
            │                    ┌──────┐
            └────────────────────│ dead │
                  requeue        └──────┘
```

Two special cases:

- **Stale `processing`**: if the process crashes mid-lead, the row stays `processing`. On startup and every few minutes, rows in `processing` for more than 5 minutes are reset to `pending`.
- **Requeue**: moves a `dead` lead back to `pending`, due immediately, with the attempt counter reset. The attempt history is kept.

---

## 4. Retry policy

Retries use exponential backoff, doubling the wait each time, with up to **5 retries** (6 attempts in total including the first).

| Event | Wait before next attempt | Time since first failure |
|---|---|---|
| Attempt 1 fails | 1 minute | 1 min |
| Attempt 2 fails | 2 minutes | 3 min |
| Attempt 3 fails | 4 minutes | 7 min |
| Attempt 4 fails | 8 minutes | 15 min |
| Attempt 5 fails | 16 minutes | 31 min |
| Attempt 6 fails | none, lead becomes `dead` | ~31 min |

The wait is always measured from the failure of the previous attempt. Implementation: after a failed attempt number `n` (1-based), if `n <= 5`, set `next_attempt_at = now + 2^(n-1) minutes` and status `pending`. If `n == 6`, set status `dead` and `died_at = now`.

**Consequence to understand:** the whole retry window is about 31 minutes. If ERPNext is down for longer, leads that arrived during the outage will become `dead`. That is expected: dead does not mean lost. The lead is still retrievable from Facebook for about 90 days, and a requeue after the outage processes it normally. If you want automatic coverage of longer outages, extend the schedule (for example 1, 2, 4, 8, 16, 32, 60 minutes) by changing the backoff table, with no other code changes.

Optional hardening: add a little random jitter (±10%) to each wait so many leads that failed together do not all retry at the same instant.

---

## 5. Error classification

Not every error deserves five retries. Each failed attempt is classified, and the class drives what happens next and what ERPNext sees.

| Class | Examples | Behaviour |
|---|---|---|
| **transient** | Timeouts, connection errors, ERPNext 5xx, Graph 5xx, rate limits (HTTP 429, Graph codes 4, 17, 32, 613) | Retry on the schedule. |
| **token** | Graph OAuth errors (code 190 and similar, code 102 session errors) | Retried on the schedule, but flagged `token`. Affects every lead of that page, so the page token needs fixing. |
| **permanent** | ERPNext 4xx validation errors (missing mandatory field, invalid value), Graph 4xx for a deleted or invalid lead | No retry. The lead goes `dead` immediately with the error saved. Retrying cannot fix it. |

Notes:

- HTTP 408 and 429 from ERPNext count as transient even though they are 4xx.
- "A Lead with this `leadgen_id` already exists in ERPNext" is **not an error**. It means a previous attempt succeeded but its response was lost, so the lead is marked `done`.
- Credentials are read at startup. After updating a page token in `credentials.json`, restart the service (or have the worker re-read the file on each attempt).

---

## 6. Database design (SQLite)

SQLite is enough for this workload. Use **WAL mode** and a **busy timeout** so the webhook writing and the worker reading do not block each other. Use one connection per thread, not a shared connection.

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS leads (
    leadgen_id        TEXT PRIMARY KEY,         -- Facebook lead ID; also dedupes webhook resends
    page_id           TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending','processing','done','dead')),
    attempts          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   TEXT NOT NULL,            -- ISO 8601 UTC
    last_error        TEXT,                     -- short text of the latest error
    last_error_stage  TEXT,                     -- 'fetch' (Graph) or 'push' (ERPNext)
    last_error_class  TEXT,                     -- 'transient' | 'token' | 'permanent'
    last_error_at     TEXT,
    requeue_count     INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    died_at           TEXT,
    completed_at      TEXT,
    payload           TEXT,                     -- JSON exactly as sent to ERPNext (see below)
    webhook_value     TEXT                      -- JSON of the webhook's change.value (fallback data)
);

CREATE INDEX IF NOT EXISTS idx_leads_due
    ON leads (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS lead_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id      TEXT NOT NULL REFERENCES leads(leadgen_id) ON DELETE CASCADE,
    attempt_no      INTEGER NOT NULL,
    stage           TEXT NOT NULL,              -- 'fetch' or 'push'
    classification  TEXT,                       -- transient | token | permanent (null on success)
    http_status     INTEGER,                    -- null for timeouts / connection errors
    exception_class TEXT,                       -- e.g. Timeout, ConnectionError, HTTPError
    response_body   TEXT,                       -- truncated to ~2 KB
    duration_ms     INTEGER,
    success         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempts_lead
    ON lead_attempts (leadgen_id, attempt_no);
```

**Payload contents:** the payload carries every field Facebook gives us for the lead:

| Source | Keys in the payload |
|---|---|
| Form answers (`field_data`) | One key per question, named exactly as in the form (`full_name`, `email`, `phone_number`, and every custom question). Multi-select answers are joined with `, `. |
| Disclaimer checkboxes (`custom_disclaimer_responses`) | `disclaimer_<checkbox_key>` = the `is_checked` value (these are not part of `field_data`). |
| Lead metadata | `created_time` (as Facebook returns it, ISO 8601), `ad_id`, `ad_name`, `adset_id`, `adset_name`, `campaign_id`, `campaign_name`, `form_id`, `is_organic`, `platform`, `partner_name` (when present). Any extra field you add to `graph_lead_fields` is copied too; nested values become JSON text. |
| Added by the service | `leadgen_id` (or your `leadgen_id_field`), `lead_source`, `page_id` |

Ad-level fields (`ad_*`, `adset_*`, `campaign_*`) require the `ads_management` permission on the token. If Facebook does not return `ad_id`, `form_id` or `created_time`, the service falls back to the values carried by the webhook notification itself (stored in `leads.webhook_value`); Graph values always win. `created_time` is not reformatted, so the ERPNext mapping must convert Facebook's `2026-10-05T08:00:00+0000` into ERPNext's `YYYY-MM-DD HH:MM:SS` for Datetime fields.

**Payload storage:** after the first successful Graph fetch, the worker saves the exact JSON it sends to ERPNext (the flattened Facebook fields plus `lead_source` and `leadgen_id`) in `leads.payload`. Consequences:

- Retries and requeues skip the Graph call and re-send the stored payload.
- A lead whose ERPNext push keeps failing still has its data even if the Facebook token breaks later.
- The payload is **personal data** (names, phones, emails). By default it is kept until the row is purged (30 days after `done`, 90 days after `dead`). Set `"clear_payload_on_done": true` to erase it as soon as the lead succeeds.
- If the Graph fetch never succeeds, `payload` stays `NULL`.
- Databases created before this column existed are migrated automatically on startup.

Why two levels of error storage:

- `leads.last_error*` gives a quick answer to "why is this lead stuck?".
- `lead_attempts` keeps the **full history**. The first error is often the most informative one (for example a 400 validation error that later retries hide behind timeouts), and a single `last_error` column overwrites it.

**What is stored per attempt:** stage (fetch or push), classification, HTTP status, exception class, response body (truncated), duration, timestamp. ERPNext's error body usually names the exact missing or invalid field, which is the most useful part.

**Rules for stored errors:**

- **Never store the Graph `access_token`.** The service passes it as a query parameter, and `requests` exceptions often include the full URL in their message. Strip `access_token=...` from any error text before saving or logging it.
- **Truncate everything** to roughly 2 KB (a proxy's HTML error page can be much larger).
- **Treat error text as personal data.** ERPNext validation errors can echo a lead's name, email or phone. Give the database file strict permissions (readable only by the service user).

**Retention:** purge `done` leads (and their attempts) after about 30 days. Keep `dead` leads until resolved, but note that after about 90 days Facebook no longer returns the lead, so older dead leads cannot be requeued and can be purged.

---

## 7. The webhook

Two routes on the same path, as in the original service.

**`GET /webhook/facebook-leads`** (verification handshake)
Facebook calls this when you register the webhook. If `hub.mode == "subscribe"` and `hub.verify_token` equals your `verify_token`, return `hub.challenge` with status 200. Otherwise return 403.

**`POST /webhook/facebook-leads`** (notifications)
1. **Verify the signature.** Facebook signs every request with the `X-Hub-Signature-256` header: `sha256=` followed by the HMAC-SHA256 of the **raw request body** using your **app secret**. Compute it over the raw bytes (before JSON parsing) and compare in constant time. Reject with 403 if it does not match. Without this check anyone who finds the URL can inject fake leads.
2. Parse the payload. For each `entry` (its `id` is the page ID) and each `change`, read `change.value.leadgen_id`.
3. If the page ID is not in your configuration, log a warning and skip it.
4. Insert the lead: `INSERT OR IGNORE INTO leads (...)` with status `pending`, `attempts = 0`, `next_attempt_at = now`, and the notification's `value` object saved in `webhook_value` (used only as a fallback for `ad_id`, `form_id` and `created_time`). The primary key makes repeated webhooks harmless.
5. Return `200` immediately. **Do not call the Graph API or ERPNext here.**

Payload shape (simplified):

```json
{
  "object": "page",
  "entry": [
    {
      "id": "<PAGE_ID>",
      "time": 1700000000,
      "changes": [
        {
          "field": "leadgen",
          "value": {
            "leadgen_id": "<LEADGEN_ID>",
            "page_id": "<PAGE_ID>",
            "form_id": "<FORM_ID>",
            "ad_id": "<AD_ID>",
            "created_time": 1700000000
          }
        }
      ]
    }
  ]
}
```

If the database insert itself fails (disk full, locked beyond the busy timeout), return a 5xx so Facebook retries the webhook. That is the one case where a non-200 is correct.

---

## 8. The worker thread

A single daemon thread started once when the app starts. It loops forever:

```
every ~5 seconds:
    reset stale 'processing' rows (older than 5 min) to 'pending'
    claim up to N due leads (status='pending' AND next_attempt_at <= now), oldest first
    for each claimed lead:
        process(lead)
```

**Claiming must be atomic.** Do the select and the update to `processing` in one transaction (`BEGIN IMMEDIATE`), or use a single `UPDATE ... WHERE leadgen_id IN (SELECT ...) RETURNING ...` (SQLite 3.35+). Even with one worker, claiming atomically means you can add a second worker thread later without two workers taking the same lead.

**`process(lead)` steps:**

1. Increment `attempts` and note the attempt number.
2. **Get the payload.** If the lead already has a stored `payload` (from an earlier attempt), use it and skip the Graph call. Otherwise **fetch** the lead from the Graph API (`GET https://graph.facebook.com/{version}/{leadgen_id}` with the page's access token and the field list from `graph_lead_fields`), flatten it as described under *Payload contents* (all form answers, disclaimer answers and metadata), add `lead_source`, `page_id` and `leadgen_id`, and **save the result to `leads.payload`**.
3. **Check ERPNext for an existing Lead** with this `leadgen_id` (see section 9). If found, mark `done` and stop.
4. **Push** the stored payload to ERPNext: `POST {ERP_BASE_URL}/api/resource/Lead`.
5. On success: set `status = 'done'`, `completed_at = now`, record a successful attempt row.
6. On any failure: record the attempt (stage, class, status code, truncated body with the token stripped, duration), then apply the retry policy from section 4. A `permanent` classification skips straight to `dead`.

**Batching:** limit how many leads are processed per loop (for example 20) so that after a long outage a requeue of hundreds of leads does not hit ERPNext all at once.

**Shutdown:** the thread is a daemon. On stop, any lead mid-processing stays `processing` and is recovered by the stale-row reset on the next start.

---

## 9. Duplicate protection (idempotency)

A retry can create a duplicate in one situation: ERPNext created the Lead, but the response was lost (timeout or network drop), so the service thinks it failed and tries again. A requeue of a dead lead has the same risk.

The fix needs two things:

1. **A `leadgen_id` custom field on the ERPNext Lead doctype** (type Data, unique if you can). The service always sends `leadgen_id` with the payload.
2. **A check before insert.** The worker asks ERPNext whether a Lead with this `leadgen_id` already exists:

```
GET {ERP_BASE_URL}/api/resource/Lead?filters=[["leadgen_id","=","<LEADGEN_ID>"]]&fields=["name"]&limit_page_length=1
```

If a record comes back, mark the lead `done` without creating another. If you mark the field unique in ERPNext, a concurrent duplicate insert will be rejected by the database as a last line of defense. The service should treat that specific duplicate-entry rejection as success.

Because the database primary key already prevents duplicate queue entries, this ERPNext-side check is the only remaining source of duplicates, and it closes that gap.

---

## 10. Admin API (for ERPNext)

A separate route prefix, `/admin`, kept apart from the public Facebook webhook.

**Authentication:** every request must send the header `X-Admin-Key: <key>`. Compare against `admin_api_key` from `credentials.json` using a constant-time comparison (`hmac.compare_digest`). Return 401 on mismatch. Use a different key from the Facebook verify token. Serve it over HTTPS only, and restrict by IP to your ERPNext server if it has a fixed address.

### 10.1 List dead leads

`GET /admin/dead-leads`

Returns **only dead leads**. Leads that are still retrying are never returned.

Query parameters (all optional):

| Parameter | Meaning |
|---|---|
| `page_id` | Only leads from this Facebook page |
| `since`, `until` | ISO 8601 range on `died_at` |
| `limit` | Page size, default 50, max 200 |
| `offset` | For paging |

Response:

```json
{
  "count": 2,
  "total": 2,
  "limit": 50,
  "offset": 0,
  "leads": [
    {
      "leadgen_id": "1234567890",
      "page_id": "1000001",
      "page_name": "Main Page",
      "attempts": 6,
      "last_error_stage": "push",
      "last_error_class": "permanent",
      "last_error": "417: Mandatory field missing: mobile_no",
      "created_at": "2026-10-05T08:00:00Z",
      "died_at": "2026-10-05T08:31:10Z",
      "requeue_count": 0
    }
  ]
}
```

The list does not include the lead's form answers. They are only returned by the detail call (10.2), so ERPNext staff can see exactly what failed and, if needed, create the lead by hand.

### 10.2 Lead detail with attempt history

`GET /admin/dead-leads/<leadgen_id>`

Returns the same fields as above, the **`payload`** (exactly what was or would be sent to ERPNext, `null` if the Graph fetch never succeeded), and the full attempt list, so someone can see the story: for example attempt 1 timed out, attempt 2 got a 500, attempt 3 got a 417 validation error.

```json
{
  "leadgen_id": "1234567890",
  "status": "dead",
  "payload": {
    "full_name": "Jo Doe", "email": "jo@example.com", "phone_number": "+911234567890",
    "created_time": "2026-10-05T08:00:00+0000",
    "campaign_id": "C1", "campaign_name": "Diwali Offer", "adset_id": "S1", "adset_name": "Punjab 25-45",
    "ad_id": "A1", "ad_name": "Video 1", "form_id": "F1", "is_organic": false, "platform": "fb",
    "page_id": "1000001", "lead_source": "Facebook Lead Ads", "leadgen_id": "1234567890"
  },
  "attempts_history": [
    {"attempt_no": 1, "stage": "push", "classification": "transient",
     "http_status": null, "exception_class": "Timeout", "duration_ms": 15001,
     "created_at": "2026-10-05T08:00:05Z"},
    {"attempt_no": 2, "stage": "push", "classification": "permanent",
     "http_status": 417, "response_body": "{\"exception\":\"MandatoryError...\"}",
     "duration_ms": 220, "created_at": "2026-10-05T08:01:07Z"}
  ]
}
```

### 10.3 Requeue

`POST /admin/requeue`

Moves matching **dead** leads back to `pending`, due immediately, with `attempts = 0` and `requeue_count + 1`. The attempt history is kept. The worker does the actual push, so retry logic, error logging and duplicate checks stay in one place. This endpoint never calls ERPNext itself.

Request body (JSON). At least one filter is required:

```json
{ "leadgen_ids": ["1234567890", "1234567891"] }
```
```json
{ "page_id": "1000001", "since": "2026-10-05T00:00:00Z", "until": "2026-10-05T12:00:00Z" }
```
```json
{ "all": true }
```

Add `"refetch": true` to any of these to discard the stored payload, so each lead is fetched from Facebook again. Use it after changing `graph_lead_fields`, or after granting a missing permission. Leads processed before such a change keep their old payload on retry unless you requeue them with `refetch`. Do not use it for leads close to the 90-day limit, since Facebook may no longer return them.

Safety rules:

- An empty body or a body with no filter is rejected with 400. Requeueing everything requires the explicit `"all": true`.
- Only rows with status `dead` are affected. The response reports how many were requeued.
- Consider capping `requeue_count` (for example refuse after 5) so a scheduled job cannot loop on a lead that keeps failing permanently.

Response:

```json
{ "requeued": 2 }
```

### 10.4 Health and stats (optional)

- `GET /health` returns `{"status": "ok"}` if the database is reachable. No personal data, so it can be left unauthenticated for uptime monitors.
- `GET /admin/stats` (authenticated, optional) returns **counts only**, never lead lists: number of leads per status, age of the oldest pending lead, time of the last successful lead, and per page the number of failures in the last hour. This gives an early warning for an expired token before leads go dead.

---

## 11. ERPNext side

### 11.1 Custom field

On the **Lead** doctype add a custom field:

- Label `Leadgen ID`, fieldname `leadgen_id`, type Data, unique if possible, read-only is fine.

### 11.2 Field mapping

The service forwards Facebook's field names unchanged. The mapping from Facebook keys (`full_name`, `email`, `phone_number`, `campaign_name`, ...) to Lead fields (`lead_name`, `email_id`, `mobile_no`, ...) happens on ERPNext. Make sure the mapping fills every mandatory Lead field (usually `lead_name`). If it does not, inserts fail with a permanent 4xx error and the lead goes dead.

If the mapping is implemented as a custom API method in an ERPNext app, change `ERP_BASE_URL/api/resource/Lead` in the worker to that method's URL (for example `/api/method/your_app.api.fb_lead`). `/api/resource/Lead` only accepts keys that already match Lead fieldnames and will not run custom mapping logic.

### 11.3 On-demand dead-lead report

A Script Report or a custom page that calls the admin API when someone opens it. Put the service URL and admin key in a protected place (a Single doctype with a Password field, or `site_config.json`), not in client-side code. Because the API key must stay on the server, have the report call a whitelisted server method, not call the service from the browser.

Example server-side methods in a custom Frappe app:

```python
import frappe
import requests

def _cfg():
    return {
        "base": frappe.conf.get("lead_service_url"),
        "key": frappe.conf.get("lead_service_admin_key"),
    }

@frappe.whitelist()
def get_dead_leads(page_id=None, limit=50, offset=0):
    frappe.only_for(["System Manager", "Sales Manager"])
    c = _cfg()
    params = {"limit": limit, "offset": offset}
    if page_id:
        params["page_id"] = page_id
    r = requests.get(
        f"{c['base']}/admin/dead-leads",
        params=params,
        headers={"X-Admin-Key": c["key"]},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()

@frappe.whitelist()
def requeue(leadgen_ids):
    frappe.only_for(["System Manager", "Sales Manager"])
    c = _cfg()
    r = requests.post(
        f"{c['base']}/admin/requeue",
        json={"leadgen_ids": frappe.parse_json(leadgen_ids)},
        headers={"X-Admin-Key": c["key"]},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()
```

The report shows one row per dead lead with page, error class, stage, last error, died time and a **Requeue** button that calls `requeue` with the selected IDs. Show the last error beside the button: if the cause was a permanent validation error, someone should fix the ERPNext mapping first, or the lead will simply die again.

### 11.4 Reading the report

| What you see | Likely cause | Action |
|---|---|---|
| Many leads from one page, class `token` | Page access token expired or revoked | Generate a new page token, update `credentials.json`, restart the service, requeue that page. |
| Class `permanent`, stage `push`, message about a mandatory field | ERPNext mapping or form change | Fix the mapping or the form, then requeue. |
| Class `transient`, stage `push`, many at the same time | ERPNext was down longer than the retry window | Confirm ERPNext is healthy, then requeue the time range. |
| Class `permanent`, stage `fetch` | Lead deleted or inaccessible on Facebook | Cannot be recovered through the service. Handle manually. |

---

## 12. Configuration (`credentials.json`)

```json
{
  "verify_token": "a-long-random-string-for-the-webhook-handshake",
  "app_secret": "facebook-app-secret-used-to-verify-signatures",
  "graph_api_version": "v26.0",
  "admin_api_key": "another-long-random-string-for-the-admin-api",
  "erpnext": {
    "base_url": "https://erp.example.com",
    "api_key": "erpnext-api-key",
    "api_secret": "erpnext-api-secret"
  },
  "pages": {
    "1000001": {
      "name": "Main Page",
      "page_access_token": "EAAB...",
      "source": "Facebook Lead Ads"
    },
    "1000002": {
      "name": "Second Page",
      "page_access_token": "EAAB...",
      "source": "Facebook - Second Page"
    }
  }
}
```

Environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `CREDENTIALS_PATH` | `credentials.json` | Path to the file above |
| `DATABASE_PATH` | `leads.db` | SQLite file location |
| `PORT` | `3000` | Port for the dev server (gunicorn uses its own `--bind`) |
| `DISABLE_WORKER` | unset | Set to `1` to run without the worker thread (tests only) |

Optional keys in `credentials.json`:

| Key | Default | Meaning |
|---|---|---|
| `clear_payload_on_done` | `false` | Erase the stored payload when a lead succeeds |
| `worker_threads` | `1` | Number of worker threads (1 to 8) |
| `max_requeues` | `5` | Max times one lead can be requeued |
| `graph_lead_fields` | see below | Fields requested from the Graph API Lead object (list or comma-separated string) |
| `alert_webhook_url` | none | Receives `{"text": "..."}` when a lead dies |
| `erpnext.leadgen_id_field` | `leadgen_id` | ERPNext custom field holding the Facebook lead ID |
| `erpnext.duplicate_check` | `true` | Look up the lead in ERPNext before inserting |

Default `graph_lead_fields`: `id, created_time, ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name, form_id, is_organic, platform, partner_name, custom_disclaimer_responses, field_data`. If Facebook rejects a field for your API version (error code 100, "nonexisting field"), the lead will fail with a permanent error and go dead. Remove the field from this list and requeue with `refetch`.

There is **no `field_map`** any more. `source` is sent as `lead_source`. Remove the line that sets it if ERPNext assigns the source itself.

Protect the file: `chmod 600 credentials.json`, owned by the service user, and keep it out of version control.

---

## 13. Facebook setup

1. Create a Facebook App (Business type) and add the **Webhooks** product.
2. Under Webhooks, choose the **Page** object, set the callback URL to `https://your-domain/webhook/facebook-leads` and the verify token to your `verify_token`. Facebook calls the GET route to verify it.
3. Subscribe to the **leadgen** field.
4. Subscribe each Page to the app (Page subscription for `leadgen`).
5. For each Page, obtain a **Page access token** with the permissions needed to read leads (`leads_retrieval`, `pages_manage_metadata`, `pages_show_list`, plus `pages_read_engagement` as required). Prefer a long-lived Page token generated from a long-lived user token or a system user. To receive the ad-level fields (`ad_*`, `adset_*`, `campaign_*`) the token must also come from a person who can advertise on the ad account and has the `ads_management` permission. Put it in `credentials.json`.
6. Copy the **App secret** (App settings → Basic) into `app_secret`.
7. Test with Facebook's **Lead Ads Testing Tool**, which sends a real webhook for a test lead.

Permission and app-review requirements change over time, so confirm them against Meta's current documentation for the Graph API version in `graph_api_version`.

---

## 14. Deployment

### 14.1 Run with one gunicorn process

Run **one worker process with several threads**:

```bash
gunicorn --workers 1 --threads 4 --bind 127.0.0.1:3000 app:app
```

Why one process:

- The webhook only inserts a row, which takes milliseconds, so one process absorbs far more webhooks than Facebook will ever send.
- The worker handles about 1 to 2 seconds per lead (roughly 30 to 60 leads per minute), plenty for tens to hundreds of leads a day. Bursts just wait briefly in the queue.
- SQLite prefers a single writer. Several processes would each start their own worker thread and contend for the file.

Do not use the Flask development server (`app.run`) in production.

### 14.2 systemd unit

```ini
[Unit]
Description=Facebook Leads to ERPNext
After=network.target

[Service]
User=leadsvc
WorkingDirectory=/opt/fb-leads
Environment=CREDENTIALS_PATH=/opt/fb-leads/credentials.json
Environment=DATABASE_PATH=/var/lib/fb-leads/leads.db
ExecStart=/opt/fb-leads/venv/bin/gunicorn --workers 1 --threads 4 --bind 127.0.0.1:3000 app:app
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

On a restart nothing is lost: leads are in SQLite, and any row left in `processing` is reset to `pending`.

### 14.3 nginx with HTTPS

Terminate TLS in nginx (for example with Let's Encrypt) and proxy to the app. Facebook requires HTTPS for webhooks, and the admin API carries personal data, so it must be HTTPS too.

```nginx
server {
    listen 443 ssl;
    server_name leads.example.com;

    # ssl_certificate / ssl_certificate_key ...

    location /webhook/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
    }

    location /admin/ {
        allow 203.0.113.10;   # your ERPNext server
        deny all;
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
    }

    location /health {
        proxy_pass http://127.0.0.1:3000;
    }
}
```

### 14.4 Backups

Back up the SQLite file only if you want to keep the history. It contains personal data (stored payloads), so protect backups like the live file. Leads can be refetched from Facebook for about 90 days, so losing the database is recoverable within that window.

---

## 15. Security

| Area | Measure |
|---|---|
| Webhook authenticity | Verify `X-Hub-Signature-256` with the app secret on every POST. |
| Webhook handshake | Verify token compared on the GET route. |
| Admin API | Separate `X-Admin-Key`, constant-time comparison, HTTPS only, IP allow-list for the ERPNext server, kept on its own `/admin` prefix. |
| Requeue abuse | Requires a filter, `all` must be explicit, optional `requeue_count` cap. |
| Secrets | `credentials.json` mode 600, never committed. Rotate tokens if exposed. |
| Token leakage | Strip `access_token` from every logged or stored error text. |
| Personal data | The database stores each lead's payload (names, phones, emails) and error bodies that may echo them. Restrict the database file (mode 600, service user only), exclude it from casual backups, consider `clear_payload_on_done`, and rely on the retention purge. |
| ERPNext credentials | Use a dedicated ERPNext API user with permission only to read and create Leads. |

---

## 16. Operations runbook

**Normal day:** nothing to do. Leads arrive, get processed in seconds, and become `done`.

**ERPNext was down:**
- Under about 31 minutes: leads recover automatically on the retry schedule.
- Longer: some leads go dead. Once ERPNext is healthy, open the dead-lead report and requeue the affected time range.

**Page token expired (class `token`):**
1. Generate a new Page access token.
2. Update `credentials.json` and restart the service.
3. Requeue that page's dead leads (`page_id` filter).

**Validation error (class `permanent`, stage `push`):**
1. Read the last error to see which field ERPNext rejected.
2. Fix the ERPNext mapping or form.
3. Requeue the leads.

**Lead cannot be fetched (`permanent`, stage `fetch`):** the lead was deleted or the form access was removed. It cannot be recovered through the service.

**Suspected duplicates:** search ERPNext Leads by `leadgen_id`. If the unique custom field and pre-check are in place, duplicates should not occur.

**Watching for trouble:** log every state change, alert when a lead dies (even a simple email or chat message), and check that the ERPNext report is opened regularly. A dead lead nobody looks at is as lost as before.

**Housekeeping:** purge `done` rows older than 30 days (and their attempts), and dead rows older than about 90 days.

---

## 17. Capacity and limits

| Aspect | Value |
|---|---|
| Webhook receive rate | Thousands per minute (just a DB insert) |
| Worker throughput | About 30 to 70 leads per minute per worker thread (each lead needs 3 sequential calls, roughly 0.8–2 s). Measured at about 68/min with simulated latencies totalling 0.85 s per lead; raise `worker_threads` for more. |
| Daily volume this suits | Tens to a few hundred leads a day, with bursts |
| Retry window | About 31 minutes (5 retries) |
| Facebook lead retention | About 90 days |
| Per-request timeouts | 15 seconds each for Graph and ERPNext |

Note on the timeouts: because the webhook never calls external services any more, a slow ERPNext cannot make Facebook resend the webhook.

---

## 18. Testing checklist

1. **Handshake:** `curl "https://host/webhook/facebook-leads?hub.mode=subscribe&hub.verify_token=<token>&hub.challenge=123"` returns `123`. A wrong token returns 403.
2. **Signature:** a POST without a valid `X-Hub-Signature-256` returns 403. A correctly signed POST returns 200.
3. **Queueing:** a signed webhook creates a `pending` row and returns 200 in milliseconds. Sending the same `leadgen_id` twice leaves one row.
4. **Happy path:** with a real test lead (Lead Ads Testing Tool), the row ends as `done` and the Lead exists in ERPNext with `leadgen_id` set.
5. **Transient failure:** stop ERPNext, send a lead, and watch the retry times (about 1, 3, 7, 15, 31 minutes after the first failure). Restart ERPNext partway and confirm it completes.
6. **Dead path:** keep ERPNext down for the whole window and confirm the lead becomes `dead` with the attempt history recorded.
7. **Permanent error:** make the ERPNext mapping reject the lead and confirm it goes dead after one attempt.
8. **Token error:** use an invalid page token and confirm the class is `token` and no token appears in logs or stored errors.
9. **Admin API:** without the key you get 401. `GET /admin/dead-leads` returns only dead leads. Detail shows attempt history.
10. **Requeue:** an empty body is rejected with 400. A filtered requeue moves the lead to `pending`, and it completes after the cause is fixed.
11. **Duplicates:** make the ERPNext response time out after creation and confirm the retry finds the existing Lead and marks it `done`.
12. **Crash recovery:** kill the process while a lead is `processing`, restart, and confirm it is picked up again.

---

## 19. Implementation checklist

- [ ] Create SQLite schema (WAL mode, busy timeout, per-thread connections)
- [ ] Webhook GET handshake
- [ ] Webhook POST: signature check, `INSERT OR IGNORE`, return 200
- [ ] Graph fetch function that flattens `field_data` and merges metadata
- [ ] ERPNext existence check by `leadgen_id`
- [ ] ERPNext push forwarding fields as-is plus `lead_source` and `leadgen_id`
- [ ] Error classifier (transient, token, permanent) and token-stripping sanitizer
- [ ] Attempt recording (`lead_attempts`) with truncated response bodies
- [ ] Worker loop with atomic claim, batch limit, stale-`processing` reset
- [ ] Backoff schedule (1, 2, 4, 8, 16 minutes) and dead transition
- [ ] Admin API: dead-leads list, detail, requeue, with `X-Admin-Key`
- [ ] Optional `/health` and `/admin/stats`
- [ ] Retention purge job
- [ ] ERPNext: `leadgen_id` custom field, mapping covers mandatory fields
- [ ] ERPNext: whitelisted methods, on-demand dead-lead report, requeue button
- [ ] gunicorn (1 worker, several threads), systemd unit, nginx with HTTPS and IP allow-list
- [ ] Dead-lead alert (email or chat)
- [ ] Run the testing checklist above

---

## 20. Future scaling

The design leaves room to grow without redesign:

- **More throughput:** add worker threads in the same process (atomic claiming already prevents double processing).
- **Longer outages:** extend the backoff table (for example up to 60-minute steps) or schedule a daily automatic requeue of recent dead leads.
- **Much higher volume:** move the worker into its own process, or replace SQLite and the thread with Redis plus RQ or Celery. The webhook logic, error classification, retry schedule and admin API stay the same.
- **Faster warning:** extend `/admin/stats` with per-page health (last success, recent failures, dominant error class) so an expired token is visible long before leads go dead.