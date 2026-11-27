"""SQLite schema, connections, attempt history and timestamp helpers."""
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

from .config import DATABASE_PATH
from .security import redact


# ---------------------------------------------------------------- time utils
_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def now_dt():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime(_TS_FMT)


def now_iso():
    return iso(now_dt())


def parse_iso(s):
    return datetime.strptime(s, _TS_FMT).replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id TEXT NOT NULL UNIQUE,
    page_id TEXT,
    form_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    requeues INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    locked_at TEXT,
    last_error TEXT,
    last_error_class TEXT,
    last_error_stage TEXT,
    payload TEXT,
    graph_data TEXT,
    webhook_data TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    done_at TEXT,
    dead_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_leads_queue ON leads(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_leads_page ON leads(page_id);
CREATE TABLE IF NOT EXISTS lead_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    leadgen_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    stage TEXT NOT NULL,
    ok INTEGER NOT NULL,
    error_class TEXT,
    http_status INTEGER,
    response_body TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_lead ON lead_attempts(leadgen_id);
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    pid INTEGER,
    last_seen TEXT NOT NULL
);
"""


def db():
    conn = sqlite3.connect(DATABASE_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    with closing(db()) as conn:
        conn.executescript(SCHEMA)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(leads)")}
        if "graph_data" not in cols:      # database created by an older version
            conn.execute("ALTER TABLE leads ADD COLUMN graph_data TEXT")


def record_attempt(conn, leadgen_id, attempt_no, stage, ok, error_class=None,
                   http_status=None, body=None, error=None):
    conn.execute(
        "INSERT INTO lead_attempts(leadgen_id,attempt_no,stage,ok,error_class,http_status,"
        "response_body,error,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (leadgen_id, attempt_no, stage, 1 if ok else 0, error_class, http_status,
         redact(body), redact(error, 1000), now_iso()))
