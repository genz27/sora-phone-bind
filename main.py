"""Sora 绑定平台 Web 服务"""
import asyncio
import json
import os
import random
import re
import sys
import threading
import uuid
import sqlite3
import hashlib
import secrets
import time
from collections import deque
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from pydantic import BaseModel
from curl_cffi.requests import AsyncSession
import uvicorn
from urllib.parse import urlparse, quote, urlunparse

app = FastAPI()

# ============== 运行目录 ==============

def get_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))


def get_runtime_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = get_base_dir()
RUNTIME_DIR = get_runtime_dir()
STATIC_DIR = os.path.join(BASE_DIR, "static")
DATA_DIR = os.path.join(RUNTIME_DIR, "data")

DB_FILE = os.path.join(DATA_DIR, "sora_bind.sqlite3")

# ============== 全局状态 ==============
DEFAULT_CONFIG = {
    "phones": [],
    "proxies": [],
    "concurrency": 3,
    "code_poll_interval": 5,
    "code_max_retries": 30,
}

CONFIG_CACHE: Dict[str, Any] = DEFAULT_CONFIG.copy()
PHONE_POOL: List[Dict[str, str]] = []
PHONE_INDEX = 0
PROXY_LIST: List[str] = []
PROXY_INDEX = 0
PHONE_LOCKED: set[str] = set()
PHONE_USAGE: Dict[str, int] = {}
PHONE_STATUS: Dict[str, str] = {}
PHONE_API: Dict[str, str] = {}

PROCESS_PAUSED = False
PROCESS_STOPPED = False
PAUSE_EVENT = asyncio.Event()
PAUSE_EVENT.set()

JOB_SEMAPHORE = asyncio.Semaphore(DEFAULT_CONFIG["concurrency"])
SCHEDULED_JOBS: set[int] = set()
SCHEDULED_LOCK = asyncio.Lock()
PHONE_LOCK = asyncio.Lock()
PHONE_REQUEST_TASKS: set[int] = set()
PHONE_REQUEST_LOCK = asyncio.Lock()
PROXY_LOCK = threading.Lock()
DB_LOCK = threading.Lock()

LOGS = deque(maxlen=200)

CARD_CODE_PATTERN = re.compile(r"^[A-Z0-9-]{8,32}$")
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._-]{10,512}$")

MAX_PHONE_BINDINGS = 3
PHONE_CODE_MAX_RETRIES = 120
PHONE_CODE_INTERVAL = 1

ADMIN_TOKEN_TTL = 60 * 60 * 12
ADMIN_TOKENS: Dict[str, float] = {}
ADMIN_SALT_KEY = "admin_password_salt"
ADMIN_HASH_KEY = "admin_password_hash"

# ============== 指纹库 ==============
MOBILE_FINGERPRINTS = [
    "safari17_2_ios",
    "safari18_0_ios",
]

MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/131.0.6778.73 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.6723.90 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.81 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.81 Mobile Safari/537.36",
]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Origin": "https://sora.chatgpt.com",
    "Pragma": "no-cache",
    "Priority": "u=1, i",
    "Referer": "https://sora.chatgpt.com/",
    "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24", "Google Chrome";v="131"',
    "Sec-Ch-Ua-Mobile": "?1",
    "Sec-Ch-Ua-Platform": '"iOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# ============== 工具函数 ==============

def log_event(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    LOGS.append(f"[{ts}] {msg}")
    print(f"[{ts}] {msg}")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def db_connect():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def db_execute(sql: str, params: Tuple[Any, ...] = ()) -> int:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute(sql, params)
        conn.commit()
        rowcount = cur.rowcount
        conn.close()
        return rowcount


def db_fetchall(sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows


def db_fetchone(sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    rows = db_fetchall(sql, params)
    return rows[0] if rows else None


def db_insert(sql: str, params: Tuple[Any, ...] = ()) -> int:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute(sql, params)
        conn.commit()
        last_id = cur.lastrowid
        conn.close()
        return last_id


def db_executemany(sql: str, params_list: List[Tuple[Any, ...]]) -> None:
    with DB_LOCK:
        conn = db_connect()
        conn.executemany(sql, params_list)
        conn.commit()
        conn.close()


def init_db() -> None:
    with DB_LOCK:
        conn = db_connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                total INTEGER NOT NULL,
                remaining INTEGER NOT NULL,
                note TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                card_code TEXT NOT NULL,
                token_type TEXT NOT NULL,
                token TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                phone TEXT,
                new_rt TEXT,
                sms_code TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS phone_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_code TEXT NOT NULL,
                phone TEXT NOT NULL,
                api_url TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                sms_code TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS phone_usage (
                phone TEXT PRIMARY KEY,
                bound_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs(batch_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_phone_requests_status ON phone_requests(status);
            """
        )
        conn.commit()
        conn.close()


def ensure_jobs_schema() -> None:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute("PRAGMA table_info(jobs)")
        cols = {row["name"] for row in cur.fetchall()}
        if "sms_code" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN sms_code TEXT")
        if "client_id" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN client_id TEXT")
        conn.commit()
        conn.close()


def ensure_phone_requests_schema() -> None:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute("PRAGMA table_info(phone_requests)")
        cols = {row["name"] for row in cur.fetchall()}
        if not cols:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS phone_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    card_code TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    api_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    sms_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        conn.commit()
        conn.close()


def ensure_phone_usage_schema() -> None:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute("PRAGMA table_info(phone_usage)")
        cols = {row["name"] for row in cur.fetchall()}
        if not cols:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS phone_usage (
                    phone TEXT PRIMARY KEY,
                    bound_count INTEGER NOT NULL,
                    api_url TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    updated_at TEXT NOT NULL
                )
                """
            )
        else:
            if "api_url" not in cols:
                conn.execute("ALTER TABLE phone_usage ADD COLUMN api_url TEXT")
            if "status" not in cols:
                conn.execute("ALTER TABLE phone_usage ADD COLUMN status TEXT DEFAULT 'active'")
        conn.commit()
        conn.close()


def ensure_phone_poll_schema() -> None:
    with DB_LOCK:
        conn = db_connect()
        cur = conn.execute("PRAGMA table_info(phone_requests_poll)")
        cols = {row["name"] for row in cur.fetchall()}
        if not cols:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS phone_requests_poll (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
        conn.commit()
        conn.close()


def get_setting(key: str, default: Any) -> Any:
    row = db_fetchone("SELECT value FROM settings WHERE key = ?", (key,))
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except json.JSONDecodeError:
        return default


def set_setting(key: str, value: Any) -> None:
    payload = json.dumps(value, ensure_ascii=False)
    db_execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, payload))


def hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def ensure_admin_password() -> None:
    salt = get_setting(ADMIN_SALT_KEY, "")
    password_hash = get_setting(ADMIN_HASH_KEY, "")
    if salt and password_hash:
        return
    password = os.getenv("SORA_ADMIN_PASSWORD", "admin123")
    salt = secrets.token_hex(16)
    password_hash = hash_password(password, salt)
    set_setting(ADMIN_SALT_KEY, salt)
    set_setting(ADMIN_HASH_KEY, password_hash)
    log_event("[AUTH] 初始化管理员密码")


def verify_admin_password(password: str) -> bool:
    salt = get_setting(ADMIN_SALT_KEY, "")
    password_hash = get_setting(ADMIN_HASH_KEY, "")
    if not salt or not password_hash:
        return False
    return hash_password(password, salt) == password_hash


def issue_admin_token() -> str:
    token = secrets.token_urlsafe(32)
    ADMIN_TOKENS[token] = time.time() + ADMIN_TOKEN_TTL
    return token


def prune_admin_tokens() -> None:
    now = time.time()
    expired = [token for token, exp in ADMIN_TOKENS.items() if exp <= now]
    for token in expired:
        ADMIN_TOKENS.pop(token, None)


async def require_admin(authorization: Optional[str] = Header(None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="未登录")
    token = authorization.split(" ", 1)[1].strip()
    prune_admin_tokens()
    exp = ADMIN_TOKENS.get(token)
    if not exp:
        raise HTTPException(status_code=401, detail="未登录")


def apply_config(cfg: Dict[str, Any]) -> None:
    global CONFIG_CACHE, PHONE_POOL, PHONE_INDEX, PROXY_LIST, PROXY_INDEX, JOB_SEMAPHORE
    CONFIG_CACHE = {
        "phones": cfg.get("phones", []),
        "proxies": cfg.get("proxies", []),
        "concurrency": max(int(cfg.get("concurrency", 3)), 1),
        "code_poll_interval": max(int(cfg.get("code_poll_interval", 5)), 1),
        "code_max_retries": max(int(cfg.get("code_max_retries", 30)), 1),
    }
    PHONE_POOL = list(CONFIG_CACHE["phones"])
    PHONE_INDEX = 0
    PROXY_LIST = list(CONFIG_CACHE["proxies"])
    PROXY_INDEX = 0
    JOB_SEMAPHORE = asyncio.Semaphore(CONFIG_CACHE["concurrency"])


def load_config() -> Dict[str, Any]:
    cfg = get_setting("config", DEFAULT_CONFIG)
    apply_config(cfg)
    return CONFIG_CACHE


def sync_phone_registry() -> None:
    phone_map = {p["phone"]: p["api"] for p in CONFIG_CACHE.get("phones", [])}
    for phone, api in phone_map.items():
        upsert_phone_usage(phone, get_phone_usage_count(phone), api, "active")
    for phone, status in list(PHONE_STATUS.items()):
        if phone not in phone_map and status != "invalid":
            set_phone_status(phone, "inactive")


def load_phone_usage() -> None:
    global PHONE_USAGE, PHONE_STATUS, PHONE_API
    rows = db_fetchall("SELECT phone, bound_count, api_url, status FROM phone_usage")
    PHONE_USAGE = {}
    PHONE_STATUS = {}
    PHONE_API = {}
    for row in rows:
        phone = row["phone"]
        PHONE_USAGE[phone] = int(row["bound_count"])
        PHONE_STATUS[phone] = row.get("status") or "active"
        if row.get("api_url"):
            PHONE_API[phone] = row["api_url"]


def normalize_lines(text: str) -> List[str]:
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


def parse_phone_text(text: str) -> List[Dict[str, str]]:
    items = []
    for line in normalize_lines(text):
        if "----" not in line:
            continue
        parts = line.split("----", 1)
        phone = parts[0].strip()
        api_url = parts[1].strip()
        if phone and api_url:
            items.append({"phone": phone, "api": api_url})
    return items


def format_phone_text(items: List[Dict[str, str]]) -> str:
    return "\n".join(f"{p['phone']}----{p['api']}" for p in items)


def validate_card_code(code: str) -> bool:
    return bool(CARD_CODE_PATTERN.fullmatch(code))


def normalize_card_code(code: str) -> str:
    code = code.strip().upper()
    if not validate_card_code(code):
        raise HTTPException(400, "卡密格式不正确")
    return code


def assert_card_match(expected: str, provided: str) -> None:
    if expected.strip().upper() != provided.strip().upper():
        raise HTTPException(status_code=401, detail="卡密不匹配")


def generate_card_code(length: int = 16) -> str:
    alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(length))


def validate_tokens(token_type: str, tokens: List[str]) -> Tuple[bool, List[str]]:
    invalid = []
    if token_type not in {"at", "rt"}:
        return False, tokens
    for token in tokens:
        if not TOKEN_PATTERN.fullmatch(token):
            invalid.append(token)
    return len(invalid) == 0, invalid


def get_card(code: str) -> Optional[Dict[str, Any]]:
    return db_fetchone("SELECT * FROM cards WHERE code = ?", (code,))


def reserve_card_credit(code: str) -> bool:
    rowcount = db_execute(
        "UPDATE cards SET remaining = remaining - 1 WHERE code = ? AND active = 1 AND remaining > 0",
        (code,),
    )
    return rowcount == 1


def refund_card_credit(code: str) -> None:
    db_execute(
        "UPDATE cards SET remaining = CASE WHEN remaining < total THEN remaining + 1 ELSE remaining END WHERE code = ?",
        (code,),
    )


def update_card(code: str, total: int, remaining: int, active: int, note: str) -> None:
    db_execute(
        "UPDATE cards SET total = ?, remaining = ?, active = ?, note = ? WHERE code = ?",
        (total, remaining, active, note, code),
    )


def insert_jobs(batch_id: str, card_code: str, token_type: str, tokens: List[str], client_id: Optional[str] = None) -> None:
    now = now_str()
    payload = [
        (batch_id, card_code, token_type, token, "pending", "待处理", "", "", client_id or "", now, now)
        for token in tokens
    ]
    db_executemany(
        """
        INSERT INTO jobs (batch_id, card_code, token_type, token, status, message, phone, new_rt, client_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def fetch_job(job_id: int) -> Optional[Dict[str, Any]]:
    return db_fetchone("SELECT * FROM jobs WHERE id = ?", (job_id,))


def fetch_jobs_by_batch(batch_id: str) -> List[Dict[str, Any]]:
    return db_fetchall(
        "SELECT * FROM jobs WHERE batch_id = ? ORDER BY id ASC",
        (batch_id,),
    )


def fetch_pending_job_ids() -> List[int]:
    rows = db_fetchall("SELECT id FROM jobs WHERE status = 'pending' ORDER BY id ASC")
    return [int(r["id"]) for r in rows]


def is_job_canceled(job_id: int) -> bool:
    row = db_fetchone("SELECT status FROM jobs WHERE id = ?", (job_id,))
    if not row:
        return False
    return row["status"] == "canceled"


def is_phone_request_canceled(request_id: int) -> bool:
    row = db_fetchone("SELECT status FROM phone_requests WHERE id = ?", (request_id,))
    if not row:
        return False
    return row["status"] == "canceled"


def get_pending_queue_map() -> Dict[int, int]:
    rows = db_fetchall("SELECT id FROM jobs WHERE status = 'pending' ORDER BY id ASC")
    return {int(r["id"]): idx + 1 for idx, r in enumerate(rows)}


def get_running_total() -> int:
    row = db_fetchone("SELECT COUNT(*) as c FROM jobs WHERE status = 'running'")
    if not row:
        return 0
    return int(row["c"])


def update_job(job_id: int, status: Optional[str] = None, message: Optional[str] = None,
               phone: Optional[str] = None, new_rt: Optional[str] = None,
               sms_code: Optional[str] = None) -> None:
    updates = []
    params: List[Any] = []
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if message is not None:
        updates.append("message = ?")
        params.append(message)
    if phone is not None:
        updates.append("phone = ?")
        params.append(phone)
    if new_rt is not None:
        updates.append("new_rt = ?")
        params.append(new_rt)
    if sms_code is not None:
        updates.append("sms_code = ?")
        params.append(sms_code)
    updates.append("updated_at = ?")
    params.append(now_str())
    params.append(job_id)
    db_execute(f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?", tuple(params))


def insert_phone_request(card_code: str, phone: str, api_url: str) -> int:
    now = now_str()
    return db_insert(
        """
        INSERT INTO phone_requests (card_code, phone, api_url, status, message, sms_code, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (card_code, phone, api_url, "waiting", "等待验证码", "", now, now),
    )


def fetch_phone_request(request_id: int) -> Optional[Dict[str, Any]]:
    return db_fetchone("SELECT * FROM phone_requests WHERE id = ?", (request_id,))


def get_phone_request_attempts(request_id: int) -> int:
    row = db_fetchone(
        "SELECT COUNT(*) as c FROM phone_requests_poll WHERE request_id = ?",
        (request_id,),
    )
    if not row:
        return 0
    return int(row["c"])


def update_phone_request(request_id: int, status: Optional[str] = None,
                         message: Optional[str] = None, sms_code: Optional[str] = None) -> None:
    updates = []
    params: List[Any] = []
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if message is not None:
        updates.append("message = ?")
        params.append(message)
    if sms_code is not None:
        updates.append("sms_code = ?")
        params.append(sms_code)
    updates.append("updated_at = ?")
    params.append(now_str())
    params.append(request_id)
    db_execute(f"UPDATE phone_requests SET {', '.join(updates)} WHERE id = ?", tuple(params))


def upsert_phone_usage(phone: str, bound_count: int, api_url: str, status: str) -> None:
    api_value = api_url or PHONE_API.get(phone, "")
    db_execute(
        "INSERT INTO phone_usage (phone, bound_count, api_url, status, updated_at) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(phone) DO UPDATE SET bound_count = excluded.bound_count, "
        "api_url = excluded.api_url, status = excluded.status, updated_at = excluded.updated_at",
        (phone, bound_count, api_value, status, now_str()),
    )
    PHONE_USAGE[phone] = int(bound_count)
    PHONE_STATUS[phone] = status
    if api_value:
        PHONE_API[phone] = api_value


def get_phone_usage_count(phone: str) -> int:
    return int(PHONE_USAGE.get(phone, 0))


def increment_phone_usage(phone: str, api_url: str) -> int:
    count = get_phone_usage_count(phone) + 1
    upsert_phone_usage(phone, count, api_url, "active")
    return count


def set_phone_status(phone: str, status: str, api_url: str = "") -> None:
    bound_count = get_phone_usage_count(phone)
    upsert_phone_usage(phone, bound_count, api_url, status)


def get_phone_usage_items() -> List[Dict[str, Any]]:
    rows = db_fetchall("SELECT phone, bound_count, api_url, status, updated_at FROM phone_usage ORDER BY updated_at DESC")
    return rows


async def handle_phone_success(phone: str, api_url: str) -> None:
    count = increment_phone_usage(phone, api_url)
    if count >= MAX_PHONE_BINDINGS:
        await remove_phone(phone, reason="绑定次数已满")


async def clear_full_phones() -> int:
    rows = db_fetchall(
        "SELECT phone FROM phone_usage WHERE bound_count >= ? AND status = 'active'",
        (MAX_PHONE_BINDINGS,),
    )
    count = 0
    for row in rows:
        phone = row["phone"]
        await remove_phone(phone, reason="一键清空已满")
        count += 1
    return count


def build_export_response(lines: List[str], filename: str, media_type: str = "text/plain") -> PlainTextResponse:
    if not lines:
        raise HTTPException(404, "无可导出数据")
    return PlainTextResponse(
        "\n".join(lines),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def normalize_pagination(page: int, page_size: int, max_size: int = 200) -> Tuple[int, int, int]:
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = 1
    page = max(page, 1)
    page_size = max(page_size, 1)
    page_size = min(page_size, max_size)
    offset = (page - 1) * page_size
    return page, page_size, offset


def csv_escape(value: Any) -> str:
    text = "" if value is None else str(value)
    if any(ch in text for ch in [",", "\"", "\n", "\r"]):
        text = text.replace("\"", "\"\"")
        return f"\"{text}\""
    return text


def get_batch_token_type(jobs: List[Dict[str, Any]]) -> str:
    if not jobs:
        return "token"
    token_type = jobs[0].get("token_type") or "token"
    return token_type.lower()


def encode_proxy_url(proxy: str) -> str:
    if not proxy:
        return proxy
    try:
        parsed = urlparse(proxy)
        if parsed.username or parsed.password:
            username = quote(parsed.username or "", safe="")
            password = quote(parsed.password or "", safe="")
            if username and password:
                netloc = f"{username}:{password}@{parsed.hostname}"
            elif username:
                netloc = f"{username}@{parsed.hostname}"
            else:
                netloc = parsed.hostname
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"
            return urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    except Exception:
        pass
    return proxy


def get_proxy() -> Optional[str]:
    global PROXY_INDEX
    if not PROXY_LIST:
        return None
    with PROXY_LOCK:
        proxy = PROXY_LIST[PROXY_INDEX % len(PROXY_LIST)]
        PROXY_INDEX += 1
    return encode_proxy_url(proxy)


def build_headers(token: str) -> dict:
    return {
        **HEADERS,
        "Authorization": f"Bearer {token}",
        "User-Agent": random.choice(MOBILE_USER_AGENTS),
        "oai-device-id": str(uuid.uuid4()),
        "Content-Type": "application/json",
    }


async def wait_if_paused() -> bool:
    while not PAUSE_EVENT.is_set():
        if PROCESS_STOPPED:
            return False
        await asyncio.sleep(0.5)
    return not PROCESS_STOPPED


async def get_next_phone() -> Optional[Dict[str, str]]:
    global PHONE_INDEX
    async with PHONE_LOCK:
        if not PHONE_POOL:
            return None
        for _ in range(len(PHONE_POOL)):
            phone = PHONE_POOL[PHONE_INDEX % len(PHONE_POOL)]
            PHONE_INDEX += 1
            if phone["phone"] not in PHONE_LOCKED:
                return phone
        return None


async def acquire_phone() -> Optional[Dict[str, str]]:
    global PHONE_INDEX
    to_remove: List[str] = []
    async with PHONE_LOCK:
        if not PHONE_POOL:
            return None
        for _ in range(len(PHONE_POOL)):
            phone = PHONE_POOL[PHONE_INDEX % len(PHONE_POOL)]
            PHONE_INDEX += 1
            if phone["phone"] in PHONE_LOCKED:
                continue
            if get_phone_usage_count(phone["phone"]) >= MAX_PHONE_BINDINGS:
                to_remove.append(phone["phone"])
                continue
            PHONE_LOCKED.add(phone["phone"])
            return phone
    for phone in to_remove:
        await remove_phone(phone, reason="绑定次数已满")
    return None


async def remove_phone(phone: str, reason: str = "") -> None:
    global PHONE_POOL, PHONE_INDEX
    async with PHONE_LOCK:
        PHONE_POOL = [p for p in PHONE_POOL if p["phone"] != phone]
        PHONE_INDEX = 0
        PHONE_LOCKED.discard(phone)
    api_url = PHONE_API.get(phone, "")
    set_phone_status(phone, "invalid", api_url)
    CONFIG_CACHE["phones"] = list(PHONE_POOL)
    cfg = CONFIG_CACHE.copy()
    set_setting("config", cfg)
    msg = f"[PHONE] {phone} 已失效"
    if reason:
        msg = f"{msg}: {reason}"
    log_event(msg)


async def lock_phone(phone: str) -> None:
    async with PHONE_LOCK:
        PHONE_LOCKED.add(phone)


async def unlock_phone(phone: str) -> None:
    async with PHONE_LOCK:
        PHONE_LOCKED.discard(phone)


# ============== 核心功能 ==============
DEFAULT_CLIENT_ID = "app_LlGpXReQgckcGGUo2JrYvtJK"


def _generate_random_username() -> str:
    """生成随机用户名"""
    import string
    chars = string.ascii_lowercase + string.digits
    return "user_" + "".join(random.choice(chars) for _ in range(8))


async def get_user_info(token: str) -> Optional[Dict[str, Any]]:
    """获取用户信息"""
    proxy = get_proxy()
    try:
        async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
            kwargs = {
                "headers": build_headers(token),
                "timeout": 30,
            }
            if proxy:
                kwargs["proxy"] = proxy
            r = await s.get("https://sora.chatgpt.com/backend/me", **kwargs)
            if r.status_code == 200:
                return r.json()
            response_text = r.text[:200] if r.text else ""
            log_event(f"[WARN] 获取用户信息失败 HTTP {r.status_code} - {response_text}")
    except Exception as e:
        log_event(f"[WARN] 获取用户信息异常: {e}")
    return None


async def check_username_available(token: str, username: str) -> bool:
    """检查用户名是否可用"""
    proxy = get_proxy()
    try:
        async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
            kwargs = {
                "headers": build_headers(token),
                "json": {"username": username},
                "timeout": 30,
            }
            if proxy:
                kwargs["proxy"] = proxy
            r = await s.post("https://sora.chatgpt.com/backend/project_y/profile/username/check", **kwargs)
            if r.status_code == 200:
                data = r.json()
                return data.get("available", False)
    except Exception as e:
        log_event(f"[WARN] 检查用户名异常: {e}")
    return False


async def set_username(token: str, username: str) -> bool:
    """设置用户名"""
    proxy = get_proxy()
    try:
        async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
            kwargs = {
                "headers": build_headers(token),
                "json": {"username": username},
                "timeout": 30,
            }
            if proxy:
                kwargs["proxy"] = proxy
            r = await s.post("https://sora.chatgpt.com/backend/project_y/profile/username/set", **kwargs)
            if r.status_code == 200:
                log_event(f"[USERNAME] 设置成功: {username}")
                return True
            log_event(f"[WARN] 设置用户名失败 HTTP {r.status_code} - {r.text[:200] if r.text else ''}")
    except Exception as e:
        log_event(f"[WARN] 设置用户名异常: {e}")
    return False


async def activate_sora2(token: str) -> bool:
    """激活Sora2账户"""
    proxy = get_proxy()
    try:
        async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
            kwargs = {
                "headers": build_headers(token),
                "timeout": 30,
            }
            if proxy:
                kwargs["proxy"] = proxy
            r = await s.get("https://sora.chatgpt.com/backend/m/bootstrap", **kwargs)
            if r.status_code == 200:
                log_event(f"[SORA2] 激活成功")
                return True
            response_text = r.text[:200] if r.text else ""
            log_event(f"[WARN] Sora2激活失败 HTTP {r.status_code} - {response_text}")
    except Exception as e:
        log_event(f"[WARN] Sora2激活异常: {e}")
    return False


async def ensure_user_activated(token: str, job_id: int) -> bool:
    """确保用户已激活Sora账户（有用户名）"""
    # 先激活Sora2
    update_job(job_id, message="激活Sora2")
    await activate_sora2(token)

    update_job(job_id, message="检查账户状态")
    user_info = await get_user_info(token)
    if not user_info:
        log_event(f"[{job_id}] 无法获取用户信息")
        return False

    username = user_info.get("username")
    if username:
        log_event(f"[{job_id}] 账户已激活 username={username}")
        return True

    log_event(f"[{job_id}] 账户未激活，开始设置用户名")
    update_job(job_id, message="设置用户名")

    max_attempts = 5
    for attempt in range(max_attempts):
        generated_username = _generate_random_username()
        log_event(f"[{job_id}] 尝试用户名 ({attempt + 1}/{max_attempts}): {generated_username}")

        if await check_username_available(token, generated_username):
            if await set_username(token, generated_username):
                log_event(f"[{job_id}] 账户激活成功 username={generated_username}")
                return True
        else:
            log_event(f"[{job_id}] 用户名 {generated_username} 已被占用")

    log_event(f"[{job_id}] 账户激活失败，达到最大尝试次数")
    return False


async def rt_to_at(rt: str, max_retries: int = 3, client_id: Optional[str] = None) -> Dict[str, Any]:
    effective_client_id = client_id or DEFAULT_CLIENT_ID
    for attempt in range(max_retries):
        proxy = get_proxy()
        try:
            async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
                kwargs = {
                    "headers": {"Accept": "application/json", "Content-Type": "application/json"},
                    "json": {
                        "client_id": effective_client_id,
                        "grant_type": "refresh_token",
                        "redirect_uri": "com.openai.chat://auth0.openai.com/ios/com.openai.chat/callback",
                        "refresh_token": rt.strip(),
                    },
                    "timeout": 30,
                }
                if proxy:
                    kwargs["proxy"] = proxy
                r = await s.post("https://auth.openai.com/oauth/token", **kwargs)
                if r.status_code != 200:
                    response_text = r.text[:200] if r.text else ""
                    raise ValueError(f"HTTP {r.status_code} - {response_text}")
                d = r.json()
                if not d.get("access_token"):
                    raise ValueError("RT无效")
                return {"access_token": d["access_token"], "refresh_token": d.get("refresh_token")}
        except Exception as e:
            log_event(f"[WARN] RT转换失败 {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
    raise ValueError("RT转换失败")


async def send_code(token: str, phone: str, max_retries: int = 3) -> Tuple[bool, Optional[str]]:
    for attempt in range(max_retries):
        proxy = get_proxy()
        try:
            async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
                kwargs = {
                    "headers": build_headers(token),
                    "json": {"phone_number": phone, "verification_expiry_window_ms": None},
                    "timeout": 30,
                }
                if proxy:
                    kwargs["proxy"] = proxy
                r = await s.post("https://sora.chatgpt.com/backend/project_y/phone_number/enroll/start", **kwargs)
                if r.status_code == 200:
                    return True, None

                response_text = r.text
                if "already verified" in response_text.lower() or "phone number already" in response_text.lower():
                    return False, "phone_used"
        except Exception as e:
            log_event(f"[WARN] 发送验证码异常 {attempt + 1}/{max_retries}: {e}")
        if attempt < max_retries - 1:
            await asyncio.sleep(2)
    return False, "other"


async def get_code(
    api_url: str,
    max_retries: int = 30,
    interval: int = 5,
    task_id: int = 0,
    stop_check: Optional[Callable[[], Awaitable[bool]]] = None,
    on_poll: Optional[Callable[[int], Awaitable[None]]] = None,
) -> Optional[str]:
    async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS), trust_env=False) as s:
        for i in range(max_retries):
            if stop_check and await stop_check():
                return None
            if on_poll:
                await on_poll(i + 1)
            try:
                r = await s.get(api_url, timeout=15)
                if r.status_code == 200:
                    d = r.json()
                    if d.get("code") == 1 and d.get("data", {}).get("code"):
                        code_str = d["data"]["code"]
                        m = re.search(r"\d{6}", code_str)
                        if m:
                            return m.group()
            except Exception as e:
                log_event(f"[{task_id}] [WARN] 获取验证码异常: {e}")
            if i < max_retries - 1:
                if stop_check and await stop_check():
                    return None
                await asyncio.sleep(interval)
    return None


async def submit_code(token: str, phone: str, code: str) -> bool:
    proxy = get_proxy()
    async with AsyncSession(impersonate=random.choice(MOBILE_FINGERPRINTS)) as s:
        kwargs = {
            "headers": build_headers(token),
            "json": {"phone_number": phone, "verification_code": code},
            "timeout": 30,
        }
        if proxy:
            kwargs["proxy"] = proxy
        r = await s.post("https://sora.chatgpt.com/backend/project_y/phone_number/enroll/finish", **kwargs)
        success = r.status_code == 200
        response_text = r.text[:200] if r.text else ""
        if success:
            log_event(f"[VERIFY] 验证成功 phone={phone} code={code}")
        else:
            log_event(f"[VERIFY] 验证失败 phone={phone} code={code} HTTP {r.status_code} - {response_text}")
        return success


async def schedule_job(job_id: int) -> None:
    async with SCHEDULED_LOCK:
        if job_id in SCHEDULED_JOBS:
            return
        SCHEDULED_JOBS.add(job_id)
    asyncio.create_task(process_job(job_id))


async def schedule_phone_request(request_id: int) -> None:
    async with PHONE_REQUEST_LOCK:
        if request_id in PHONE_REQUEST_TASKS:
            return
        PHONE_REQUEST_TASKS.add(request_id)
    asyncio.create_task(process_phone_request(request_id))


async def process_job(job_id: int) -> None:
    reserved = False
    completed = False
    card_code = ""
    locked_phone = ""
    try:
        async with JOB_SEMAPHORE:
            job = fetch_job(job_id)
            if not job or job.get("status") != "pending":
                return

            if PROCESS_STOPPED:
                update_job(job_id, status="failed", message="任务已停止")
                return

            update_job(job_id, status="running", message="处理中")
            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            if not await wait_if_paused():
                update_job(job_id, status="failed", message="任务已停止")
                return

            card_code = job["card_code"]
            card = get_card(card_code)
            if not card or not card["active"]:
                update_job(job_id, status="failed", message="卡密无效或已禁用")
                return
            if card["remaining"] <= 0:
                update_job(job_id, status="failed", message="卡密额度不足")
                return

            token_type = job["token_type"]
            token = job["token"]
            original_rt = token if token_type == "rt" else ""
            client_id = job.get("client_id") or None
            new_rt = ""

            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            if token_type == "rt":
                update_job(job_id, message="RT转换中")
                at_info = await rt_to_at(token, client_id=client_id)
                token = at_info["access_token"]
                new_rt = at_info.get("refresh_token") or ""
                original_rt = new_rt or original_rt  # 使用新RT
                if is_job_canceled(job_id):
                    update_job(job_id, status="canceled", message="已取消")
                    return

            # 检查并激活账户
            if not await ensure_user_activated(token, job_id):
                update_job(job_id, status="failed", message="账户激活失败")
                return

            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            if not await wait_if_paused():
                update_job(job_id, status="failed", message="任务已停止")
                return

            phone_info = await acquire_phone()
            if not phone_info:
                update_job(job_id, status="failed", message="无可用手机号")
                return

            phone = phone_info["phone"]
            api_url = phone_info["api"]
            locked_phone = phone

            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            if not reserve_card_credit(card_code):
                update_job(job_id, status="failed", message="卡密额度不足")
                return
            reserved = True
            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            update_job(job_id, message="发送验证码", phone=phone)
            success, error_type = await send_code(token, phone)
            if not success:
                if error_type == "phone_used":
                    await unlock_phone(phone)
                    locked_phone = ""
                    await remove_phone(phone, reason="手机号已被使用")
                    phone_info = await acquire_phone()
                    if not phone_info:
                        update_job(job_id, status="failed", message="无可用手机号")
                        return
                    phone = phone_info["phone"]
                    api_url = phone_info["api"]
                    locked_phone = phone
                    update_job(job_id, message="更换手机号发送", phone=phone)
                    success, error_type = await send_code(token, phone)
                    if not success:
                        if error_type == "phone_used":
                            await unlock_phone(phone)
                            locked_phone = ""
                            await remove_phone(phone, reason="手机号已被使用")
                        update_job(job_id, status="failed", message="发送失败", phone=phone)
                        return
                else:
                    update_job(job_id, status="failed", message="发送失败", phone=phone)
                    return

            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消")
                return

            update_job(job_id, message="等待验证码", phone=phone)
            async def job_cancel_check() -> bool:
                return is_job_canceled(job_id)

            code = await get_code(
                api_url,
                max_retries=CONFIG_CACHE["code_max_retries"],
                interval=CONFIG_CACHE["code_poll_interval"],
                task_id=job_id,
                stop_check=job_cancel_check,
            )
            if not code:
                if is_job_canceled(job_id):
                    update_job(job_id, status="canceled", message="已取消", phone=phone)
                    return
                update_job(job_id, status="failed", message="获取验证码超时", phone=phone)
                return

            update_job(job_id, message=f"验证码: {code}", phone=phone, sms_code=code)
            await handle_phone_success(phone, api_url)
            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消", phone=phone)
                return

            # RT类型需要重新获取AT（等待验证码期间AT可能已过期）
            if token_type == "rt" and original_rt:
                update_job(job_id, message="刷新Token", phone=phone)
                try:
                    at_info = await rt_to_at(original_rt, client_id=client_id)
                    token = at_info["access_token"]
                    if at_info.get("refresh_token"):
                        new_rt = at_info["refresh_token"]
                        original_rt = new_rt
                except Exception as e:
                    log_event(f"[{job_id}] 刷新Token失败: {e}")
                    update_job(job_id, status="failed", message=f"刷新Token失败: {e}", phone=phone)
                    return

            update_job(job_id, message="提交验证", phone=phone)
            if not await submit_code(token, phone, code):
                update_job(job_id, status="failed", message="验证失败", phone=phone)
                return

            if is_job_canceled(job_id):
                update_job(job_id, status="canceled", message="已取消", phone=phone)
                return

            update_job(job_id, status="success", message="绑定成功", phone=phone, new_rt=new_rt)
            log_event(f"[OK] 绑定成功 {job_id} {token_type} {phone}")
            completed = True
    except Exception as e:
        update_job(job_id, status="failed", message=f"异常: {e}")
    finally:
        if reserved and not completed and card_code:
            refund_card_credit(card_code)
        if locked_phone:
            await unlock_phone(locked_phone)
        async with SCHEDULED_LOCK:
            SCHEDULED_JOBS.discard(job_id)


async def process_phone_request(request_id: int) -> None:
    completed = False
    card_code = ""
    phone = ""
    try:
        req = fetch_phone_request(request_id)
        if not req or req.get("status") != "waiting":
            return
        if not await wait_if_paused():
            update_phone_request(request_id, status="failed", message="任务已停止")
            return
        card_code = req["card_code"]
        phone = req["phone"]
        api_url = req["api_url"]
        update_phone_request(request_id, status="running", message="等待验证码")

        async def phone_cancel_check() -> bool:
            return is_phone_request_canceled(request_id)

        async def record_poll(attempt: int) -> None:
            db_execute(
                "INSERT INTO phone_requests_poll (request_id, created_at) VALUES (?, ?)",
                (request_id, now_str()),
            )
            remaining = max(PHONE_CODE_MAX_RETRIES - attempt, 0)
            update_phone_request(request_id, message=f"等待验证码（剩余{remaining}次）")

        code = await get_code(
            api_url,
            max_retries=PHONE_CODE_MAX_RETRIES,
            interval=PHONE_CODE_INTERVAL,
            task_id=request_id,
            stop_check=phone_cancel_check,
            on_poll=record_poll,
        )
        if not code:
            if is_phone_request_canceled(request_id):
                update_phone_request(request_id, status="canceled", message="已取消")
                return
            update_phone_request(request_id, status="failed", message="获取验证码超时")
            return

        update_phone_request(request_id, status="success", message="验证码已获取", sms_code=code)
        await handle_phone_success(phone, api_url)
        completed = True
    except Exception as e:
        update_phone_request(request_id, status="failed", message=f"异常: {e}")
    finally:
        if not completed and card_code:
            refund_card_credit(card_code)
        if phone:
            await unlock_phone(phone)
        async with PHONE_REQUEST_LOCK:
            PHONE_REQUEST_TASKS.discard(request_id)


# ============== API Models ==============
class SubmitInput(BaseModel):
    card_code: str
    token_type: str
    tokens: str
    client_id: Optional[str] = None


class BatchInput(BaseModel):
    batch_id: str
    card_code: str


class CardCreateInput(BaseModel):
    count: int
    code: Optional[str] = None
    note: Optional[str] = None


class CardAdjustInput(BaseModel):
    delta: int


class CardToggleInput(BaseModel):
    active: bool


class ConfigInput(BaseModel):
    phones: str = ""
    proxies: str = ""
    concurrency: int = 3
    code_poll_interval: int = 5
    code_max_retries: int = 30


class AdminLoginInput(BaseModel):
    password: str


class PhoneRequestInput(BaseModel):
    card_code: str


class PhoneCancelInput(BaseModel):
    request_id: int
    card_code: str


class AdminPasswordChangeInput(BaseModel):
    old_password: str
    new_password: str


# ============== 页面 ==============
@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/admin")
async def admin_page():
    return FileResponse(os.path.join(STATIC_DIR, "admin.html"))


# ============== 用户 API ==============
@app.post("/api/user/submit")
async def user_submit(inp: SubmitInput):
    card_code = normalize_card_code(inp.card_code)
    token_type = inp.token_type.strip().lower()

    card = get_card(card_code)
    if not card or not card["active"]:
        raise HTTPException(400, "卡密无效或已禁用")
    if card["remaining"] <= 0:
        raise HTTPException(400, "卡密次数不足")

    tokens = normalize_lines(inp.tokens)
    if not tokens:
        raise HTTPException(400, "未提供有效token")

    ok, invalid = validate_tokens(token_type, tokens)
    if not ok:
        return JSONResponse(
            status_code=400,
            content={"detail": "token格式不正确", "invalid_lines": invalid[:5]},
        )

    batch_id = uuid.uuid4().hex
    client_id = (inp.client_id or "").strip() or None
    insert_jobs(batch_id, card_code, token_type, tokens, client_id=client_id)
    job_ids = [j["id"] for j in fetch_jobs_by_batch(batch_id)]

    for job_id in job_ids:
        await schedule_job(job_id)

    log_event(f"[NEW] 新提交 {batch_id} {token_type} x{len(tokens)}")
    return {
        "success": True,
        "batch_id": batch_id,
        "count": len(tokens),
        "card_remaining": card["remaining"],
    }


@app.get("/api/user/batch/{batch_id}")
async def user_batch(batch_id: str, card_code: str):
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    card_code = normalize_card_code(card_code)
    assert_card_match(jobs[0]["card_code"], card_code)

    card = get_card(jobs[0]["card_code"])
    pending_map = get_pending_queue_map()
    queue_total = len(pending_map)
    running_total = get_running_total()
    counts = {
        "total": len(jobs),
        "success": len([j for j in jobs if j["status"] == "success"]),
        "failed": len([j for j in jobs if j["status"] in ("failed", "canceled")]),
        "running": len([j for j in jobs if j["status"] == "running"]),
        "pending": len([j for j in jobs if j["status"] == "pending"]),
    }

    items = []
    for j in jobs:
        token = j["token"]
        preview = token[:6] + "..." + token[-4:] if len(token) > 12 else token
        items.append(
            {
                "id": j["id"],
                "token_type": j["token_type"],
                "token_preview": preview,
                "status": j["status"],
                "message": j.get("message", ""),
                "phone": j.get("phone", ""),
                "new_rt": j.get("new_rt", ""),
                "sms_code": j.get("sms_code", ""),
                "queue_position": pending_map.get(int(j["id"]), 0),
            }
        )

    return {
        "batch_id": batch_id,
        "items": items,
        "counts": counts,
        "queue_total": queue_total,
        "running_total": running_total,
        "concurrency": CONFIG_CACHE["concurrency"],
        "card_remaining": card["remaining"] if card else 0,
        "card_total": card["total"] if card else 0,
        "card_active": bool(card["active"]) if card else False,
    }


@app.post("/api/user/clear_batch")
async def user_clear_batch(inp: BatchInput):
    card_code = normalize_card_code(inp.card_code)
    job = db_fetchone("SELECT card_code FROM jobs WHERE batch_id = ? LIMIT 1", (inp.batch_id,))
    if not job:
        raise HTTPException(404, "批次不存在")
    assert_card_match(job["card_code"], card_code)
    count = db_execute("DELETE FROM jobs WHERE batch_id = ?", (inp.batch_id,))
    return {"success": True, "deleted": count}


@app.get("/api/user/export/new_rt")
async def user_export_new_rt(batch_id: str, card_code: str):
    card_code = normalize_card_code(card_code)
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    assert_card_match(jobs[0]["card_code"], card_code)
    lines = [j["new_rt"] for j in jobs if j["status"] == "success" and j.get("new_rt")]
    return build_export_response(lines, "new_rt.txt")


@app.get("/api/user/export/failed")
async def user_export_failed(batch_id: str, card_code: str):
    card_code = normalize_card_code(card_code)
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    assert_card_match(jobs[0]["card_code"], card_code)
    lines = [j["token"] for j in jobs if j["status"] in ("failed", "canceled")]
    token_type = get_batch_token_type(jobs)
    return build_export_response(lines, f"failed_{token_type}.txt")


@app.get("/api/user/export/success")
async def user_export_success(batch_id: str, card_code: str):
    card_code = normalize_card_code(card_code)
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    assert_card_match(jobs[0]["card_code"], card_code)
    lines = [j["token"] for j in jobs if j["status"] == "success"]
    token_type = get_batch_token_type(jobs)
    return build_export_response(lines, f"success_{token_type}.txt")


@app.get("/api/user/export/unbound")
async def user_export_unbound(batch_id: str, card_code: str):
    card_code = normalize_card_code(card_code)
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    assert_card_match(jobs[0]["card_code"], card_code)
    lines = [j["token"] for j in jobs if j["status"] != "success"]
    token_type = get_batch_token_type(jobs)
    return build_export_response(lines, f"unbound_{token_type}.txt")


@app.get("/api/user/export/all_tokens")
async def user_export_all_tokens(batch_id: str, card_code: str):
    card_code = normalize_card_code(card_code)
    jobs = fetch_jobs_by_batch(batch_id)
    if not jobs:
        raise HTTPException(404, "批次不存在")
    assert_card_match(jobs[0]["card_code"], card_code)
    lines = [j["token"] for j in jobs]
    token_type = get_batch_token_type(jobs)
    return build_export_response(lines, f"all_{token_type}.txt")


@app.get("/api/user/card/{code}")
async def user_card_info(code: str):
    code = normalize_card_code(code)
    card = get_card(code)
    if not card or not card["active"]:
        raise HTTPException(404, "卡密无效或已禁用")
    return {
        "code": code,
        "remaining": card["remaining"],
        "total": card["total"],
        "active": bool(card["active"]),
    }


@app.post("/api/user/cancel_batch")
async def user_cancel_batch(inp: BatchInput):
    card_code = normalize_card_code(inp.card_code)
    job = db_fetchone("SELECT card_code FROM jobs WHERE batch_id = ? LIMIT 1", (inp.batch_id,))
    if not job:
        raise HTTPException(404, "批次不存在")
    assert_card_match(job["card_code"], card_code)
    rows = db_fetchall("SELECT id, status FROM jobs WHERE batch_id = ?", (inp.batch_id,))
    if not rows:
        raise HTTPException(404, "批次不存在")
    now = now_str()
    count = db_execute(
        """
        UPDATE jobs
        SET status = 'canceled', message = '用户终止', updated_at = ?
        WHERE batch_id = ? AND status IN ('pending', 'running')
        """,
        (now, inp.batch_id),
    )
    return {"success": True, "canceled": count}


@app.post("/api/user/phone/request")
async def user_phone_request(inp: PhoneRequestInput):
    card_code = normalize_card_code(inp.card_code)
    card = get_card(card_code)
    if not card or not card["active"]:
        raise HTTPException(400, "卡密无效或已禁用")

    phone_info = await acquire_phone()
    if not phone_info:
        raise HTTPException(400, "无可用手机号")

    if not reserve_card_credit(card_code):
        raise HTTPException(400, "卡密额度不足")

    phone = phone_info["phone"]
    api_url = phone_info["api"]
    try:
        request_id = insert_phone_request(card_code, phone, api_url)
    except Exception:
        await unlock_phone(phone)
        refund_card_credit(card_code)
        raise
    await schedule_phone_request(request_id)
    card = get_card(card_code)
    return {
        "request_id": request_id,
        "phone": phone,
        "card_remaining": card["remaining"] if card else 0,
    }


@app.get("/api/user/phone/status/{request_id}")
async def user_phone_status(request_id: int, card_code: str):
    req = fetch_phone_request(request_id)
    if not req:
        raise HTTPException(404, "请求不存在")
    card_code = normalize_card_code(card_code)
    assert_card_match(req["card_code"], card_code)
    card = get_card(req["card_code"])
    attempts = get_phone_request_attempts(request_id)
    remaining = max(PHONE_CODE_MAX_RETRIES - attempts, 0)
    return {
        "request_id": request_id,
        "phone": req.get("phone", ""),
        "status": req.get("status", ""),
        "message": req.get("message", ""),
        "sms_code": req.get("sms_code", ""),
        "poll_remaining": remaining,
        "card_remaining": card["remaining"] if card else 0,
        "card_total": card["total"] if card else 0,
    }


@app.post("/api/user/phone/cancel")
async def user_phone_cancel(inp: PhoneCancelInput):
    req = fetch_phone_request(inp.request_id)
    if not req:
        raise HTTPException(404, "请求不存在")
    card_code = normalize_card_code(inp.card_code)
    assert_card_match(req["card_code"], card_code)
    status = req.get("status")
    if status in ("success", "failed", "canceled"):
        return {"success": True, "status": status}
    update_phone_request(inp.request_id, status="canceled", message="用户终止")
    if status == "waiting":
        refund_card_credit(req["card_code"])
        await unlock_phone(req["phone"])
    return {"success": True, "status": "canceled"}


# ============== 管理员 API ==============
@app.post("/api/admin/login")
async def admin_login(inp: AdminLoginInput):
    if not verify_admin_password(inp.password):
        raise HTTPException(status_code=401, detail="密码错误")
    token = issue_admin_token()
    return {"token": token, "expires_in": ADMIN_TOKEN_TTL}


@app.post("/api/admin/password")
async def admin_change_password(inp: AdminPasswordChangeInput, _: None = Depends(require_admin)):
    if not verify_admin_password(inp.old_password):
        raise HTTPException(status_code=401, detail="原密码错误")
    new_password = inp.new_password.strip()
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="新密码至少6位")
    salt = secrets.token_hex(16)
    password_hash = hash_password(new_password, salt)
    set_setting(ADMIN_SALT_KEY, salt)
    set_setting(ADMIN_HASH_KEY, password_hash)
    ADMIN_TOKENS.clear()
    log_event("[AUTH] 修改管理员密码")
    return {"success": True}


@app.get("/api/admin/overview")
async def admin_overview(_: None = Depends(require_admin)):
    cards = db_fetchall("SELECT COUNT(*) as c FROM cards")
    active_cards = db_fetchall("SELECT COUNT(*) as c FROM cards WHERE active = 1")
    jobs = db_fetchall("SELECT COUNT(*) as c FROM jobs")
    success = db_fetchall("SELECT COUNT(*) as c FROM jobs WHERE status = 'success'")
    failed = db_fetchall("SELECT COUNT(*) as c FROM jobs WHERE status = 'failed'")
    pending = db_fetchall("SELECT COUNT(*) as c FROM jobs WHERE status = 'pending'")
    running = db_fetchall("SELECT COUNT(*) as c FROM jobs WHERE status = 'running'")
    return {
        "cards": cards[0]["c"],
        "active_cards": active_cards[0]["c"],
        "jobs": jobs[0]["c"],
        "success": success[0]["c"],
        "failed": failed[0]["c"],
        "pending": pending[0]["c"],
        "running": running[0]["c"],
        "paused": PROCESS_PAUSED,
        "concurrency": CONFIG_CACHE["concurrency"],
    }


@app.get("/api/admin/cards")
async def admin_cards(
    page: int = 1,
    page_size: int = 50,
    keyword: Optional[str] = None,
    status: Optional[str] = None,
    remaining: Optional[str] = None,
    _: None = Depends(require_admin),
):
    filters = []
    params: List[Any] = []
    keyword = (keyword or "").strip()
    status = (status or "").strip().lower()
    remaining = (remaining or "").strip().lower()
    if keyword:
        like = f"%{keyword}%"
        filters.append("(code LIKE ? OR note LIKE ?)")
        params.extend([like, like])
    if status == "active":
        filters.append("active = 1")
    elif status == "inactive":
        filters.append("active = 0")
    if remaining == "zero":
        filters.append("remaining <= 0")
    elif remaining == "nonzero":
        filters.append("remaining > 0")
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    total_row = db_fetchone(f"SELECT COUNT(*) as c FROM cards{where}", tuple(params)) or {"c": 0}
    total = int(total_row["c"])
    page, page_size, offset = normalize_pagination(page, page_size, max_size=200)
    rows = db_fetchall(
        f"SELECT * FROM cards{where} ORDER BY id DESC LIMIT ? OFFSET ?",
        tuple(params + [page_size, offset]),
    )
    return {"items": rows, "total": total, "page": page, "page_size": page_size}


@app.get("/api/admin/cards/export")
async def admin_export_cards(
    keyword: Optional[str] = None,
    status: Optional[str] = None,
    remaining: Optional[str] = None,
    mode: str = "codes",
    _: None = Depends(require_admin),
):
    filters = []
    params: List[Any] = []
    keyword = (keyword or "").strip()
    status = (status or "").strip().lower()
    remaining = (remaining or "").strip().lower()
    if keyword:
        like = f"%{keyword}%"
        filters.append("(code LIKE ? OR note LIKE ?)")
        params.extend([like, like])
    if status == "active":
        filters.append("active = 1")
    elif status == "inactive":
        filters.append("active = 0")
    if remaining == "zero":
        filters.append("remaining <= 0")
    elif remaining == "nonzero":
        filters.append("remaining > 0")
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    rows = db_fetchall(f"SELECT * FROM cards{where} ORDER BY id DESC", tuple(params))
    if not rows:
        raise HTTPException(404, "无可导出数据")
    if mode.lower() == "full":
        lines = ["code,remaining,total,active,note,created_at"]
        for row in rows:
            values = [
                row.get("code", ""),
                row.get("remaining", 0),
                row.get("total", 0),
                1 if row.get("active") else 0,
                row.get("note", ""),
                row.get("created_at", ""),
            ]
            lines.append(",".join(csv_escape(value) for value in values))
        return build_export_response(lines, "cards_full.csv", media_type="text/csv")
    lines = [row.get("code", "") for row in rows]
    return build_export_response(lines, "cards.txt")


@app.post("/api/admin/cards")
async def admin_create_card(inp: CardCreateInput, _: None = Depends(require_admin)):
    count = int(inp.count)
    if count <= 0:
        raise HTTPException(400, "可用次数必须大于0")

    code = (inp.code or "").strip().upper()
    if code:
        if not validate_card_code(code):
            raise HTTPException(400, "卡密格式不正确")
        if get_card(code):
            raise HTTPException(400, "卡密已存在")
    else:
        code = generate_card_code()

    db_insert(
        "INSERT INTO cards (code, total, remaining, note, active, created_at) VALUES (?, ?, ?, ?, 1, ?)",
        (code, count, count, inp.note or "", now_str()),
    )
    log_event(f"[CARD] 新卡密 {code} 次数 {count}")
    return {"success": True, "code": code}


@app.post("/api/admin/cards/{code}/adjust")
async def admin_adjust_card(code: str, inp: CardAdjustInput, _: None = Depends(require_admin)):
    code = code.strip().upper()
    card = get_card(code)
    if not card:
        raise HTTPException(404, "卡密不存在")
    delta = int(inp.delta)
    if delta == 0:
        return {"success": True}

    total = max(card["total"] + delta, 0)
    remaining = max(card["remaining"] + delta, 0)
    if remaining > total:
        remaining = total
    update_card(code, total, remaining, card["active"], card.get("note") or "")
    log_event(f"[CARD] 调整卡密 {code} delta {delta}")
    return {"success": True, "total": total, "remaining": remaining}


@app.post("/api/admin/cards/{code}/toggle")
async def admin_toggle_card(code: str, inp: CardToggleInput, _: None = Depends(require_admin)):
    code = code.strip().upper()
    card = get_card(code)
    if not card:
        raise HTTPException(404, "卡密不存在")
    update_card(code, card["total"], card["remaining"], 1 if inp.active else 0, card.get("note") or "")
    return {"success": True}


@app.post("/api/admin/cards/clear_zero")
async def admin_clear_zero_cards(_: None = Depends(require_admin)):
    count = db_execute("DELETE FROM cards WHERE remaining <= 0")
    if count:
        log_event(f"[CARD] 清理无次数卡密 {count}")
    return {"success": True, "deleted": count}


@app.get("/api/admin/config")
async def admin_get_config(_: None = Depends(require_admin)):
    cfg = CONFIG_CACHE
    return {
        "phones": format_phone_text(cfg["phones"]),
        "proxies": "\n".join(cfg["proxies"]),
        "concurrency": cfg["concurrency"],
        "code_poll_interval": cfg["code_poll_interval"],
        "code_max_retries": cfg["code_max_retries"],
    }


@app.post("/api/admin/config")
async def admin_set_config(inp: ConfigInput, _: None = Depends(require_admin)):
    cfg = {
        "phones": parse_phone_text(inp.phones),
        "proxies": normalize_lines(inp.proxies),
        "concurrency": max(int(inp.concurrency), 1),
        "code_poll_interval": max(int(inp.code_poll_interval), 1),
        "code_max_retries": max(int(inp.code_max_retries), 1),
    }
    set_setting("config", cfg)
    apply_config(cfg)
    sync_phone_registry()
    log_event("[CFG] 更新后台配置")
    return {"success": True, "phones": len(cfg["phones"]), "proxies": len(cfg["proxies"])}


@app.get("/api/admin/jobs")
async def admin_jobs(
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    limit: Optional[int] = None,
    keyword: Optional[str] = None,
    _: None = Depends(require_admin),
):
    filters = []
    params: List[Any] = []
    status = (status or "").strip().lower()
    keyword = (keyword or "").strip()
    if status and status != "all":
        filters.append("status = ?")
        params.append(status)
    if keyword:
        like = f"%{keyword}%"
        filters.append("(card_code LIKE ? OR phone LIKE ? OR token LIKE ? OR message LIKE ?)")
        params.extend([like, like, like, like])
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    total_row = db_fetchone(f"SELECT COUNT(*) as c FROM jobs{where}", tuple(params)) or {"c": 0}
    total = int(total_row["c"])
    if limit:
        page = 1
        page_size = limit
    page, page_size, offset = normalize_pagination(page, page_size, max_size=500)
    rows = db_fetchall(
        f"SELECT * FROM jobs{where} ORDER BY id DESC LIMIT ? OFFSET ?",
        tuple(params + [page_size, offset]),
    )
    return {"items": rows, "total": total, "page": page, "page_size": page_size}


@app.get("/api/admin/logs")
async def admin_logs(_: None = Depends(require_admin)):
    return {"items": list(LOGS)}


@app.get("/api/admin/phone_usage")
async def admin_phone_usage(_: None = Depends(require_admin)):
    return {"items": get_phone_usage_items(), "max": MAX_PHONE_BINDINGS}


@app.get("/api/admin/phones")
async def admin_phones(
    page: int = 1,
    page_size: int = 50,
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    _: None = Depends(require_admin),
):
    filters = []
    params: List[Any] = []
    status = (status or "").strip().lower()
    keyword = (keyword or "").strip()
    if status in {"active", "inactive", "invalid"}:
        filters.append("status = ?")
        params.append(status)
    if keyword:
        like = f"%{keyword}%"
        filters.append("(phone LIKE ? OR api_url LIKE ?)")
        params.extend([like, like])
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    total_row = db_fetchone(f"SELECT COUNT(*) as c FROM phone_usage{where}", tuple(params)) or {"c": 0}
    total = int(total_row["c"])
    page, page_size, offset = normalize_pagination(page, page_size, max_size=200)
    rows = db_fetchall(
        f"SELECT phone, bound_count, api_url, status, updated_at FROM phone_usage{where} "
        "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        tuple(params + [page_size, offset]),
    )
    items = []
    for row in rows:
        row_status = row.get("status") or "active"
        bound = int(row.get("bound_count") or 0)
        remaining = max(0, MAX_PHONE_BINDINGS - bound) if row_status == "active" else 0
        items.append({
            "phone": row.get("phone", ""),
            "api_url": row.get("api_url", ""),
            "bound_count": bound,
            "remaining": remaining,
            "status": row_status,
            "updated_at": row.get("updated_at", ""),
        })
    return {
        "items": items,
        "max": MAX_PHONE_BINDINGS,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.post("/api/admin/phones/clear_full")
async def admin_clear_full_phones(_: None = Depends(require_admin)):
    cleared = await clear_full_phones()
    return {"success": True, "cleared": cleared}


@app.post("/api/admin/pause")
async def admin_pause(_: None = Depends(require_admin)):
    global PROCESS_PAUSED
    PROCESS_PAUSED = True
    PAUSE_EVENT.clear()
    log_event("[PAUSE] 任务已暂停")
    return {"success": True}


@app.post("/api/admin/resume")
async def admin_resume(_: None = Depends(require_admin)):
    global PROCESS_PAUSED, PROCESS_STOPPED
    PROCESS_PAUSED = False
    PROCESS_STOPPED = False
    PAUSE_EVENT.set()
    log_event("[RESUME] 任务已恢复")
    return {"success": True}


@app.post("/api/admin/stop")
async def admin_stop(_: None = Depends(require_admin)):
    global PROCESS_STOPPED, PROCESS_PAUSED
    PROCESS_STOPPED = True
    PROCESS_PAUSED = False
    PAUSE_EVENT.set()
    log_event("[STOP] 任务已停止")
    return {"success": True}


@app.post("/api/admin/clear_jobs")
async def admin_clear_jobs(_: None = Depends(require_admin)):
    count = db_execute("DELETE FROM jobs")
    return {"success": True, "deleted": count}


# ============== 启动 ==============
@app.on_event("startup")
async def startup_event():
    init_db()
    ensure_jobs_schema()
    ensure_phone_requests_schema()
    ensure_phone_poll_schema()
    ensure_phone_usage_schema()
    ensure_admin_password()
    load_config()
    load_phone_usage()
    sync_phone_registry()
    db_execute("UPDATE jobs SET status = 'pending' WHERE status = 'running'")
    db_execute("UPDATE phone_requests SET status = 'waiting' WHERE status = 'running'")
    pending = fetch_pending_job_ids()
    for job_id in pending:
        await schedule_job(job_id)
    phone_rows = db_fetchall("SELECT id FROM phone_requests WHERE status = 'waiting'")
    for row in phone_rows:
        await schedule_phone_request(int(row["id"]))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8899)
