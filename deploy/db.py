# -*- coding: utf-8 -*-
"""Схема БД: коды, активации, HWID. Поддержка SQLite и PostgreSQL (DATABASE_URL)."""
import sqlite3
import os
import re
from contextlib import contextmanager

_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
_USE_PG = bool(_DATABASE_URL and "postgres" in _DATABASE_URL.lower())

# DB_PATH: для SQLite — явно задай или используй Railway Volume (RAILWAY_VOLUME_MOUNT_PATH)
def _get_db_path() -> str:
    if os.environ.get("DB_PATH"):
        return os.environ["DB_PATH"]
    mount = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    if mount:
        return os.path.join(mount, "voicer_licenses.db")
    return "voicer_licenses.db"

DB_PATH = _get_db_path()


def _pg_adapt_sql(sql: str) -> str:
    """Преобразует SQLite-синтаксис в PostgreSQL."""
    sql = sql.replace("?", "%s")
    sql = sql.replace("datetime('now')", "CURRENT_TIMESTAMP")
    sql = re.sub(r"datetime\('now',\s*'-1 hour'\)", "CURRENT_TIMESTAMP - INTERVAL '1 hour'", sql)
    # INSERT OR IGNORE
    if "INSERT OR IGNORE" in sql.upper():
        sql = sql.replace("INSERT OR IGNORE", "INSERT")
        if "INTO settings " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (key) DO NOTHING", sql)
        elif "INTO pending_users " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (username) DO NOTHING", sql)
        elif "INTO users " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (telegram_id) DO NOTHING", sql)
        elif "INTO referrals " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (referred_id) DO NOTHING", sql)
    # INSERT OR REPLACE
    if "INSERT OR REPLACE" in sql.upper():
        sql = sql.replace("INSERT OR REPLACE", "INSERT")
        if "INTO admins " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (telegram_id) DO UPDATE SET username=EXCLUDED.username, added_by=EXCLUDED.added_by", sql)
        elif "INTO pending_code_assign " in sql:
            sql = re.sub(r"\)\s*$", ") ON CONFLICT (admin_id) DO UPDATE SET code=EXCLUDED.code, created_at=EXCLUDED.created_at", sql)
    return sql


class _PgCursorWrapper:
    """Обёртка курсора для PostgreSQL: конвертирует ? в %s и INSERT OR IGNORE/REPLACE."""
    def __init__(self, cur):
        self._cur = cur
        self._last_inserted_id = None

    def execute(self, sql, params=None):
        self._last_inserted_id = None
        sql = _pg_adapt_sql(sql)
        if params:
            self._cur.execute(sql, params)
        else:
            self._cur.execute(sql)
        # Для INSERT с RETURNING сохраняем id
        if "RETURNING" in sql.upper():
            row = self._cur.fetchone()
            if row:
                self._last_inserted_id = row[0]
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        if self._last_inserted_id is not None:
            return self._last_inserted_id
        return self._cur.lastrowid if hasattr(self._cur, 'lastrowid') else None


class _PgConnWrapper:
    """Обёртка соединения: cursor() возвращает _PgCursorWrapper (psycopg2.cursor read-only)."""
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _PgCursorWrapper(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def _get_conn():
    if _USE_PG:
        import psycopg2
        conn = psycopg2.connect(_DATABASE_URL)
        return _PgConnWrapper(conn)
    return sqlite3.connect(DB_PATH)


@contextmanager
def get_db():
    conn = _get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _alter_safe(conn, cur, sql):
    """Выполнить ALTER TABLE, игнорируя ошибку «колонка уже есть». SAVEPOINT чтобы не откатывать всю транзакцию."""
    try:
        cur.execute("SAVEPOINT alter_safe")
        cur.execute(sql)
    except (sqlite3.OperationalError, Exception):
        try:
            cur.execute("ROLLBACK TO SAVEPOINT alter_safe")
        except Exception:
            conn.rollback()
    else:
        try:
            cur.execute("RELEASE SAVEPOINT alter_safe")
        except Exception:
            pass


def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        if _USE_PG:
            _init_db_pg(conn, cur)
        else:
            _init_db_sqlite(conn, cur)
        _ensure_partner_admins_from_env(conn)
        conn.commit()


def _init_db_sqlite(conn, cur):
    """Схема для SQLite."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            days INTEGER NOT NULL,
            is_developer INTEGER DEFAULT 0,
            assigned_username TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _alter_safe(conn, cur, "ALTER TABLE codes ADD COLUMN assigned_username TEXT")
    _alter_safe(conn, cur, "ALTER TABLE users ADD COLUMN is_gift INTEGER DEFAULT 0")
    _alter_safe(conn, cur, "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_id INTEGER NOT NULL,
            hwid TEXT NOT NULL,
            installation_id TEXT,
            user_telegram_id INTEGER,
            activated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            revoked INTEGER DEFAULT 0,
            FOREIGN KEY (code_id) REFERENCES codes(id)
        )
    """)
    _alter_safe(conn, cur, "ALTER TABLE activations ADD COLUMN installation_id TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_activations_hwid ON activations(hwid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_activations_code ON activations(code_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            added_by INTEGER NOT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            referred_by INTEGER,
            is_partner INTEGER DEFAULT 0,
            is_gift INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            custom_discount_pct REAL,
            first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referred_by) REFERENCES users(telegram_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (referrer_id) REFERENCES users(telegram_id),
            FOREIGN KEY (referred_id) REFERENCES users(telegram_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_telegram_id INTEGER NOT NULL,
            amount_usd REAL NOT NULL,
            plan_days INTEGER NOT NULL,
            code_id INTEGER,
            status TEXT DEFAULT 'confirmed',
            merchant_order_id TEXT,
            payment_system TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (code_id) REFERENCES codes(id)
        )
    """)
    for col in ("merchant_order_id", "payment_system"):
        _alter_safe(conn, cur, f"ALTER TABLE payments ADD COLUMN {col} TEXT")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referral_payouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            payment_id INTEGER NOT NULL,
            amount_usd REAL NOT NULL,
            percent REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            paid_at TEXT,
            FOREIGN KEY (referrer_id) REFERENCES users(telegram_id),
            FOREIGN KEY (payment_id) REFERENCES payments(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_referral_payouts_referrer ON referral_payouts(referrer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_telegram_id)")
    for k, v in [
        ("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил»."),
        ("price_30", "15"), ("price_60", "25"), ("price_90", "35"),
        ("software_url", "https://drive.google.com/drive/folders/18hdLnr_zPo7_Eao9thFQkp2H4nbgtLIa"),
        ("payments_enabled", "0"), ("manual_payment_contact", "@Drykey"),
    ]:
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_code_assign (
            admin_id INTEGER, code TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (admin_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            username TEXT PRIMARY KEY,
            is_blocked INTEGER DEFAULT 0,
            is_partner INTEGER DEFAULT 0,
            is_gift INTEGER DEFAULT 0,
            custom_discount_pct REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _init_db_pg(conn, cur):
    """Схема для PostgreSQL."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            days INTEGER NOT NULL,
            is_developer INTEGER DEFAULT 0,
            assigned_username TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _alter_safe(conn, cur, "ALTER TABLE codes ADD COLUMN assigned_username TEXT")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activations (
            id SERIAL PRIMARY KEY,
            code_id INTEGER NOT NULL REFERENCES codes(id),
            hwid TEXT NOT NULL,
            installation_id TEXT,
            user_telegram_id INTEGER,
            activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            revoked INTEGER DEFAULT 0
        )
    """)
    _alter_safe(conn, cur, "ALTER TABLE activations ADD COLUMN installation_id TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_activations_hwid ON activations(hwid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_activations_code ON activations(code_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            telegram_id BIGINT PRIMARY KEY,
            username TEXT,
            added_by BIGINT NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            username TEXT,
            referred_by BIGINT REFERENCES users(telegram_id),
            is_partner INTEGER DEFAULT 0,
            is_gift INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            custom_discount_pct REAL,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _alter_safe(conn, cur, "ALTER TABLE users ADD COLUMN is_gift INTEGER DEFAULT 0")
    _alter_safe(conn, cur, "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL REFERENCES users(telegram_id),
            referred_id BIGINT NOT NULL UNIQUE REFERENCES users(telegram_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_telegram_id BIGINT NOT NULL,
            amount_usd REAL NOT NULL,
            plan_days INTEGER NOT NULL,
            code_id INTEGER REFERENCES codes(id),
            status TEXT DEFAULT 'confirmed',
            merchant_order_id TEXT,
            payment_system TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for col in ("merchant_order_id", "payment_system"):
        _alter_safe(conn, cur, f"ALTER TABLE payments ADD COLUMN {col} TEXT")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referral_payouts (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL REFERENCES users(telegram_id),
            payment_id INTEGER NOT NULL REFERENCES payments(id),
            amount_usd REAL NOT NULL,
            percent REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            paid_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_referral_payouts_referrer ON referral_payouts(referrer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_telegram_id)")
    for k, v in [
        ("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил»."),
        ("price_30", "15"), ("price_60", "25"), ("price_90", "35"),
        ("software_url", "https://drive.google.com/drive/folders/18hdLnr_zPo7_Eao9thFQkp2H4nbgtLIa"),
        ("payments_enabled", "0"), ("manual_payment_contact", "@Drykey"),
    ]:
        cur.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING", (k, v))
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_code_assign (
            admin_id BIGINT PRIMARY KEY,
            code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_users (
            username TEXT PRIMARY KEY,
            is_blocked INTEGER DEFAULT 0,
            is_partner INTEGER DEFAULT 0,
            is_gift INTEGER DEFAULT 0,
            custom_discount_pct REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _ensure_partner_admins_from_env(conn):
    """Добавить партнёров из PARTNER_USER_IDS в admins (полные права как у владельца)."""
    ids_str = os.environ.get("PARTNER_USER_IDS", "").strip()
    if not ids_str:
        return
    owner = get_owner_id() or 0
    cur = conn.cursor()
    for part in ids_str.split(","):
        part = part.strip()
        if part and part.isdigit():
            tid = int(part)
            if tid != owner:
                cur.execute("INSERT OR REPLACE INTO admins (telegram_id, username, added_by) VALUES (?, ?, ?)",
                            (tid, None, owner))


def get_owner_id():
    """ID владельца (первый в списке ADMIN_USER_IDS)."""
    ids = os.environ.get("ADMIN_USER_IDS", "").strip()
    if not ids:
        return None
    try:
        return int(ids.split(",")[0].strip())
    except ValueError:
        return None


def get_all_admin_ids() -> list[int]:
    """Все ID из ADMIN_USER_IDS — полный доступ к админ-панели (владелец + партнёры)."""
    ids = os.environ.get("ADMIN_USER_IDS", "").strip()
    if not ids:
        return []
    result = []
    for part in ids.split(","):
        part = part.strip()
        if part and part.isdigit():
            result.append(int(part))
    return result


def add_admin(telegram_id: int, username: str | None, added_by: int) -> bool:
    if telegram_id == get_owner_id():
        return False
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO admins (telegram_id, username, added_by) VALUES (?, ?, ?)",
                    (telegram_id, username, added_by))
    return True


def remove_admin(telegram_id: int) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM admins WHERE telegram_id = ?", (telegram_id,))
        return cur.rowcount > 0


def list_admins():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT telegram_id, username, added_at FROM admins ORDER BY added_at")
        return [{"telegram_id": r[0], "username": r[1], "added_at": r[2]} for r in cur.fetchall()]


def is_appointed_admin(telegram_id: int) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM admins WHERE telegram_id = ?", (telegram_id,))
        return cur.fetchone() is not None


def create_code(days: int, is_developer: bool = False) -> str:
    import secrets
    code = secrets.token_hex(8).upper()[:16]
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO codes (code, days, is_developer) VALUES (?, ?, ?)",
            (code, 0 if is_developer else days, 1 if is_developer else 0)
        )
    return code


def create_codes_batch(count: int, days: int = 0, is_developer: bool = False) -> list:
    return [create_code(days, is_developer) for _ in range(count)]


def get_code_by_value(code: str) -> dict | None:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, code, days, is_developer, assigned_username FROM codes WHERE code = ?", (code,))
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "code": row[1], "days": row[2], "is_developer": bool(row[3]), "assigned_username": row[4] if len(row) > 4 else None}


def set_pending_code_assign(admin_id: int, code: str):
    """Сохранить ожидающую привязку кода (fallback при потере context)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO pending_code_assign (admin_id, code, created_at) VALUES (?, ?, datetime('now'))", (admin_id, code))


def get_pending_code_assign(admin_id: int) -> str | None:
    """Получить код, ожидающий привязки (старше 1 часа — удалить)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT code FROM pending_code_assign WHERE admin_id = ? AND datetime(created_at) > datetime('now', '-1 hour')", (admin_id,))
        row = cur.fetchone()
        return row[0] if row else None


def clear_pending_code_assign(admin_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM pending_code_assign WHERE admin_id = ?", (admin_id,))


def ensure_pending_user(username: str) -> None:
    """Создать запись в pending_users при выдаче кода (если ещё нет в users)."""
    un = (username or "").strip().lstrip("@").lower()
    if not un:
        return
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO pending_users (username, is_blocked, is_partner, is_gift) VALUES (?, 0, 0, 0)", (un,))


def get_pending_user(username: str) -> dict | None:
    un = (username or "").strip().lstrip("@").lower()
    if not un:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT username, is_blocked, is_partner, is_gift, custom_discount_pct FROM pending_users WHERE username = ?", (un,))
        row = cur.fetchone()
    if not row:
        return None
    pct = row[4] if row[4] is not None else (20.0 if row[2] else 10.0)
    return {"username": row[0], "is_blocked": bool(row[1]), "is_partner": bool(row[2]), "is_gift": bool(row[3]), "custom_discount_pct": row[4], "percent": pct}


def set_pending_blocked(username: str, is_blocked: bool) -> bool:
    ensure_pending_user(username)
    un = (username or "").strip().lstrip("@").lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE pending_users SET is_blocked = ? WHERE username = ?", (1 if is_blocked else 0, un))
        return cur.rowcount > 0


def set_pending_partner(username: str, is_partner: bool) -> bool:
    ensure_pending_user(username)
    un = (username or "").strip().lstrip("@").lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE pending_users SET is_partner = ?, is_gift = 0 WHERE username = ?", (1 if is_partner else 0, un))
        return cur.rowcount > 0


def set_pending_gift(username: str, is_gift: bool) -> bool:
    ensure_pending_user(username)
    un = (username or "").strip().lstrip("@").lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE pending_users SET is_gift = ?, is_partner = 0 WHERE username = ?", (1 if is_gift else 0, un))
        return cur.rowcount > 0


def set_pending_discount(username: str, percent: float | None) -> bool:
    ensure_pending_user(username)
    un = (username or "").strip().lstrip("@").lower()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE pending_users SET custom_discount_pct = ? WHERE username = ?", (percent, un))
        return cur.rowcount > 0


def merge_pending_to_user(telegram_id: int, username: str) -> None:
    """При первом заходе: скопировать pending → users, удалить pending."""
    un = (username or "").strip().lstrip("@").lower()
    if not un:
        return
    pend = get_pending_user(un)
    if pend:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET is_blocked = ?, is_partner = ?, is_gift = ?, custom_discount_pct = ? WHERE telegram_id = ?",
                        (1 if pend["is_blocked"] else 0, 1 if pend["is_partner"] else 0, 1 if pend["is_gift"] else 0, pend.get("custom_discount_pct"), telegram_id))
            cur.execute("DELETE FROM pending_users WHERE username = ?", (un,))


def set_code_assigned(code: str, username: str | None) -> bool:
    rec = get_code_by_value(code)
    if not rec:
        return False
    un = (username or "").strip().lstrip("@")
    if "t.me/" in un:
        un = un.split("t.me/")[-1].split("/")[0].split("?")[0]
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE codes SET assigned_username = ? WHERE id = ?", (un if un else None, rec["id"]))
    if un:
        ensure_pending_user(un)
    return True


def delete_code(code: str) -> bool:
    rec = get_code_by_value(code)
    if not rec:
        return False
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM activations WHERE code_id = ?", (rec["id"],))
        cur.execute("DELETE FROM codes WHERE id = ?", (rec["id"],))
    return True


def delete_all_codes() -> int:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM codes")
        n = cur.fetchone()[0]
        cur.execute("DELETE FROM activations")
        cur.execute("DELETE FROM codes")
        return n


def get_activation_by_code_and_hwid(code: str, hwid: str, installation_id: str | None = None) -> dict | None:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT a.id, a.expires_at, a.revoked, c.is_developer, a.installation_id
            FROM activations a JOIN codes c ON c.id = a.code_id
            WHERE c.code = ? AND a.hwid = ?
        """, (code, hwid))
        row = cur.fetchone()
        if not row:
            return None
        act_installation_id = row[4] if len(row) > 4 else None
        if act_installation_id and installation_id and act_installation_id != installation_id:
            return None
        return {"id": row[0], "expires_at": row[1], "revoked": bool(row[2]), "is_developer": bool(row[3])}


def activate_code(code: str, hwid: str, installation_id: str | None = None, user_telegram_id: int | None = None) -> dict:
    rec = get_code_by_value(code)
    if not rec:
        return {"ok": False, "error": "invalid_code"}
    existing = get_activation_by_code_and_hwid(code, hwid, installation_id)
    if existing:
        if existing["revoked"]:
            return {"ok": False, "error": "revoked"}
        if not existing["is_developer"] and existing["expires_at"]:
            from datetime import datetime
            if datetime.fromisoformat(existing["expires_at"]) < datetime.utcnow():
                return {"ok": False, "error": "expired"}
        return {"ok": True, "expires_at": existing["expires_at"], "is_developer": existing["is_developer"]}
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT a.id FROM activations a JOIN codes c ON c.id = a.code_id WHERE c.code = ? AND a.revoked = 0", (code,))
        if cur.fetchone():
            return {"ok": False, "error": "code_already_used"}
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    expires_at = None if rec["is_developer"] else (now + timedelta(days=rec["days"])).isoformat()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO activations (code_id, hwid, installation_id, user_telegram_id, expires_at) VALUES (?, ?, ?, ?, ?)",
            (rec["id"], hwid, installation_id or None, user_telegram_id, expires_at)
        )
    return {"ok": True, "expires_at": expires_at, "is_developer": rec["is_developer"]}


def check_license(code: str, hwid: str, installation_id: str | None = None) -> dict:
    rec = get_code_by_value(code)
    if not rec:
        return {"ok": False, "error": "invalid_code"}
    act = get_activation_by_code_and_hwid(code, hwid, installation_id)
    if not act:
        return {"ok": False, "error": "not_activated"}
    if act["revoked"]:
        return {"ok": False, "error": "revoked"}
    if act["is_developer"]:
        return {"ok": True, "expires_at": None, "is_developer": True}
    if not act["expires_at"]:
        return {"ok": True, "expires_at": None, "is_developer": False}
    from datetime import datetime
    if datetime.fromisoformat(act["expires_at"]) < datetime.utcnow():
        return {"ok": False, "error": "expired"}
    return {"ok": True, "expires_at": act["expires_at"], "is_developer": False}


def revoke_code(code: str) -> bool:
    rec = get_code_by_value(code)
    if not rec:
        return False
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE activations SET revoked = 1 WHERE code_id = ?", (rec["id"],))
    return True


def get_code_activation_status(code: str) -> dict | None:
    rec = get_code_by_value(code)
    if not rec:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT hwid, activated_at, revoked FROM activations WHERE code_id = ? ORDER BY activated_at DESC LIMIT 1", (rec["id"],))
        row = cur.fetchone()
    if not row or not row[0]:
        return {"status": "free", "hwid": None, "activated_at": None, "revoked": False}
    return {"status": "revoked" if row[2] else "activated", "hwid": row[0], "activated_at": row[1], "revoked": bool(row[2])}


# --- Users & Referrals ---

def ensure_user(telegram_id: int, username: str | None = None, referred_by: int | None = None) -> dict:
    """Создаёт пользователя если нет, возвращает данные. При referred_by — создаёт связь referral."""
    if referred_by:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO users (telegram_id, username, referred_by) VALUES (?, ?, ?)", (referred_by, "", None))
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT telegram_id, referred_by, is_partner, custom_discount_pct FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        if row:
            if referred_by and not row[1]:
                cur.execute("UPDATE users SET referred_by = ?, username = COALESCE(NULLIF(username,''), ?) WHERE telegram_id = ?", (referred_by, username or "", telegram_id))
                cur.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (referred_by, telegram_id))
            elif username:
                cur.execute("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
            return {"telegram_id": row[0], "referred_by": row[1], "is_partner": bool(row[2]), "custom_discount_pct": row[3]}
        cur.execute(
            "INSERT INTO users (telegram_id, username, referred_by) VALUES (?, ?, ?)",
            (telegram_id, username or "", referred_by if referred_by else None)
        )
        if referred_by:
            cur.execute("INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)", (referred_by, telegram_id))
    return {"telegram_id": telegram_id, "referred_by": referred_by, "is_partner": False, "custom_discount_pct": None}


def set_partner(telegram_id: int, is_partner: bool) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_partner = ?, is_gift = 0 WHERE telegram_id = ?", (1 if is_partner else 0, telegram_id))
        return cur.rowcount > 0


def set_gift(telegram_id: int, is_gift: bool) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_gift = ?, is_partner = 0 WHERE telegram_id = ?", (1 if is_gift else 0, telegram_id))
        return cur.rowcount > 0


def set_client_status(telegram_id: int, status: str) -> bool:
    """status: client|partner|gift"""
    if status == "partner":
        return set_partner(telegram_id, True)
    if status == "gift":
        return set_gift(telegram_id, True)
    if status == "client":
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET is_partner = 0, is_gift = 0 WHERE telegram_id = ?", (telegram_id,))
            return cur.rowcount > 0
    return False


def set_blocked(telegram_id: int, is_blocked: bool) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_blocked = ? WHERE telegram_id = ?", (1 if is_blocked else 0, telegram_id))
        return cur.rowcount > 0


def set_custom_discount(telegram_id: int, percent: float | None) -> bool:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET custom_discount_pct = ? WHERE telegram_id = ?", (percent, telegram_id))
        return cur.rowcount > 0


def get_user_by_username(username: str) -> dict | None:
    """Поиск по @username (без @)."""
    un = (username or "").strip().lstrip("@").lower()
    if not un:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT telegram_id, username, referred_by, is_partner, custom_discount_pct FROM users WHERE LOWER(REPLACE(username, '@', '')) = ?", (un,))
        row = cur.fetchone()
        if not row:
            return None
        return {"telegram_id": row[0], "username": row[1], "referred_by": row[2], "is_partner": bool(row[3]), "custom_discount_pct": row[4]}


def get_user(telegram_id: int) -> dict | None:
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT telegram_id, username, referred_by, is_partner, custom_discount_pct, first_seen, COALESCE(is_gift,0), COALESCE(is_blocked,0) FROM users WHERE telegram_id = ?", (telegram_id,))
        except sqlite3.OperationalError:
            cur.execute("SELECT telegram_id, username, referred_by, is_partner, custom_discount_pct, first_seen FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "telegram_id": row[0], "username": row[1], "referred_by": row[2], "is_partner": bool(row[3]),
            "custom_discount_pct": row[4], "first_seen": row[5] if len(row) > 5 else None,
            "is_gift": bool(row[6]) if len(row) > 6 else False,
            "is_blocked": bool(row[7]) if len(row) > 7 else False,
        }


def get_referral_percent(telegram_id: int) -> float:
    """10% клиент/подарок, 20% партнёр. custom_discount_pct переопределяет."""
    u = get_user(telegram_id)
    if not u:
        return 10.0
    if u.get("custom_discount_pct") is not None:
        return float(u["custom_discount_pct"])
    return 20.0 if u.get("is_partner") else 10.0


def get_user_status_label(telegram_id: int) -> str:
    """Клиент/Партнёр/Подарок."""
    u = get_user(telegram_id)
    if not u:
        return "Клиент"
    if u.get("is_blocked"):
        return "Заблокирован"
    if u.get("is_gift"):
        return "Подарок"
    return "Партнёр" if u.get("is_partner") else "Клиент"


def list_referrals(referrer_id: int) -> list:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT r.referred_id, u.username, r.created_at
            FROM referrals r
            LEFT JOIN users u ON u.telegram_id = r.referred_id
            WHERE r.referrer_id = ?
            ORDER BY r.created_at DESC
        """, (referrer_id,))
        return [{"telegram_id": r[0], "username": r[1], "created_at": r[2]} for r in cur.fetchall()]


def payment_exists_by_order_id(merchant_order_id: str) -> bool:
    """Проверяет, был ли платёж с таким order_id уже обработан (защита от дублей)."""
    if not merchant_order_id:
        return False
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM payments WHERE merchant_order_id = ?", (merchant_order_id,))
        return cur.fetchone() is not None


def add_payment(user_telegram_id: int, amount_usd: float, plan_days: int, code_id: int | None = None,
                merchant_order_id: str | None = None, payment_system: str | None = None) -> int:
    """Записывает платёж и начисляет реферальные выплаты. Возвращает payment_id."""
    with get_db() as conn:
        cur = conn.cursor()
        if _USE_PG:
            cur.execute(
                """INSERT INTO payments (user_telegram_id, amount_usd, plan_days, code_id, merchant_order_id, payment_system)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (user_telegram_id, amount_usd, plan_days, code_id, merchant_order_id, payment_system)
            )
            pid = cur.lastrowid
        else:
            cur.execute(
                """INSERT INTO payments (user_telegram_id, amount_usd, plan_days, code_id, merchant_order_id, payment_system)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_telegram_id, amount_usd, plan_days, code_id, merchant_order_id, payment_system)
            )
            pid = cur.lastrowid
        u = get_user(user_telegram_id)
        if u and u.get("referred_by"):
            referrer_id = u["referred_by"]
            pct = get_referral_percent(referrer_id)
            amount = round(amount_usd * pct / 100, 2)
            if amount > 0:
                cur.execute(
                    "INSERT INTO referral_payouts (referrer_id, payment_id, amount_usd, percent) VALUES (?, ?, ?, ?)",
                    (referrer_id, pid, amount, pct)
                )
        conn.commit()
    return pid


def get_referral_stats() -> list:
    """Статистика по всем рефералам: кто сколько привёл, ставка, сколько должны."""
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT u.telegram_id, u.username, u.is_partner, COALESCE(u.is_gift,0), u.custom_discount_pct,
                       (SELECT COUNT(*) FROM referrals r WHERE r.referrer_id = u.telegram_id) as ref_count,
                       (SELECT COALESCE(SUM(amount_usd), 0) FROM referral_payouts rp WHERE rp.referrer_id = u.telegram_id AND rp.status = 'pending') as pending
                FROM users u
                WHERE u.telegram_id IN (SELECT referrer_id FROM referrals)
                ORDER BY ref_count DESC
            """)
        except (sqlite3.OperationalError, Exception):
            cur.execute("""
                SELECT u.telegram_id, u.username, u.is_partner, u.custom_discount_pct,
                       (SELECT COUNT(*) FROM referrals r WHERE r.referrer_id = u.telegram_id) as ref_count,
                       (SELECT COALESCE(SUM(amount_usd), 0) FROM referral_payouts rp WHERE rp.referrer_id = u.telegram_id AND rp.status = 'pending') as pending
                FROM users u
                WHERE u.telegram_id IN (SELECT referrer_id FROM referrals)
                ORDER BY ref_count DESC
            """)
        rows = cur.fetchall()
    result = []
    n_cols = len(rows[0]) if rows else 0
    for r in rows:
        if n_cols >= 7:
            pct = r[4] if r[4] is not None else (20.0 if r[2] else 10.0)
            result.append({
                "telegram_id": r[0], "username": r[1], "is_partner": bool(r[2]), "is_gift": bool(r[3]),
                "custom_discount_pct": r[4], "ref_count": r[5], "pending_usd": round(float(r[6] or 0), 2),
                "percent": pct
            })
        else:
            pct = r[3] if r[3] is not None else (20.0 if r[2] else 10.0)
            result.append({
                "telegram_id": r[0], "username": r[1], "is_partner": bool(r[2]), "is_gift": False,
                "custom_discount_pct": r[3], "ref_count": r[4], "pending_usd": round(float(r[5] or 0), 2),
                "percent": pct
            })
    return result


def get_user_payouts(telegram_id: int) -> list:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT rp.id, rp.amount_usd, rp.percent, rp.status, rp.created_at, p.plan_days
            FROM referral_payouts rp
            JOIN payments p ON p.id = rp.payment_id
            WHERE rp.referrer_id = ?
            ORDER BY rp.created_at DESC
            LIMIT 50
        """, (telegram_id,))
        return [{"id": r[0], "amount_usd": r[1], "percent": r[2], "status": r[3], "created_at": r[4], "plan_days": r[5]} for r in cur.fetchall()]


def get_user_total_pending(telegram_id: int) -> float:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(SUM(amount_usd), 0) FROM referral_payouts WHERE referrer_id = ? AND status = 'pending'", (telegram_id,))
        return round(float(cur.fetchone()[0] or 0), 2)


def list_all_users() -> list:
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT telegram_id, username, referred_by, is_partner, COALESCE(is_gift,0), COALESCE(is_blocked,0), first_seen FROM users ORDER BY first_seen DESC")
        except sqlite3.OperationalError:
            cur.execute("SELECT telegram_id, username, referred_by, is_partner, first_seen FROM users ORDER BY first_seen DESC")
            return [{"telegram_id": r[0], "username": r[1], "referred_by": r[2], "is_partner": bool(r[3]), "is_gift": False, "is_blocked": False, "first_seen": r[4]} for r in cur.fetchall()]
        return [{"telegram_id": r[0], "username": r[1], "referred_by": r[2], "is_partner": bool(r[3]), "is_gift": bool(r[4]) if len(r) > 4 else False, "is_blocked": bool(r[5]) if len(r) > 5 else False, "first_seen": r[6] if len(r) > 6 else None} for r in cur.fetchall()]


def list_assigned_usernames_not_in_users() -> list:
    """Юзернеймы с привязанными кодами, которых ещё нет в users."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT LOWER(REPLACE(COALESCE(c.assigned_username,''), '@', ''))
            FROM codes c
            WHERE c.assigned_username IS NOT NULL AND c.assigned_username != ''
            AND NOT EXISTS (SELECT 1 FROM activations a WHERE a.code_id = c.id AND a.revoked = 0)
            AND NOT EXISTS (SELECT 1 FROM users u WHERE LOWER(REPLACE(COALESCE(u.username,''), '@', '')) = LOWER(REPLACE(c.assigned_username, '@', '')))
        """)
        return [r[0] for r in cur.fetchall() if r[0]]


def list_clients_with_extended(sort_by: str = "date") -> list:
    """Все клиенты: из users + assigned (ещё не заходили). sort_by: date|name|status."""
    users = list_all_users()
    assigned = list_assigned_usernames_not_in_users()
    seen = {str(u.get("username") or "").lower().lstrip("@") for u in users if u.get("username")}
    for un in assigned:
        if un and un not in seen:
            users.append({"telegram_id": 0, "username": un, "referred_by": None, "is_partner": False, "is_gift": False, "is_blocked": False, "first_seen": None, "_assigned_only": True})
    if sort_by == "name":
        users.sort(key=lambda u: (u.get("username") or str(u["telegram_id"])).lower())
    elif sort_by == "status":
        def _st(u):
            if u.get("is_blocked"): return 0
            if u.get("is_partner"): return 1
            if u.get("is_gift"): return 2
            return 3
        users.sort(key=_st)
    else:
        users.sort(key=lambda u: u.get("first_seen") or "", reverse=True)
    return users


def get_client_full_info(telegram_id: int, username: str | None = None) -> dict | None:
    """Полная информация о клиенте: профиль, подписка, рефералы, процент."""
    if telegram_id == 0 and username:
        return _get_client_info_assigned_only(username)
    u = get_user(telegram_id)
    if not u:
        return None
    sub = get_user_subscription_info(telegram_id, u.get("username"))
    ref_count = len(list_referrals(telegram_id))
    pending = get_user_total_pending(telegram_id)
    pct = get_referral_percent(telegram_id)
    referrer = None
    if u.get("referred_by"):
        ref_u = get_user(u["referred_by"])
        if ref_u:
            referrer = f"@{ref_u.get('username') or ref_u['telegram_id']}"
    days_left = None
    if sub:
        if sub["is_developer"]:
            days_left = "∞"
        elif sub["expires_at"]:
            from datetime import datetime
            exp = datetime.fromisoformat(sub["expires_at"])
            days_left = max(0, (exp - datetime.utcnow()).days)
    return {
        "telegram_id": u["telegram_id"],
        "username": u.get("username") or "",
        "referred_by": u.get("referred_by"),
        "referrer": referrer,
        "is_partner": u.get("is_partner", False),
        "is_gift": u.get("is_gift", False),
        "is_blocked": u.get("is_blocked", False),
        "custom_discount_pct": u.get("custom_discount_pct"),
        "first_seen": u.get("first_seen"),
        "ref_count": ref_count,
        "pending_usd": pending,
        "percent": pct,
        "subscription": sub,
        "days_left": days_left,
        "_assigned_only": False,
    }


def _get_client_info_assigned_only(username: str) -> dict:
    """Инфо для пользователя, которому выдан код, но он ещё не заходил."""
    sub = get_user_subscription_info(0, username)
    pend = get_pending_user(username)
    if pend:
        pct = pend["percent"]
        return {
            "telegram_id": 0,
            "username": username,
            "referred_by": None,
            "referrer": None,
            "is_partner": pend["is_partner"],
            "is_gift": pend["is_gift"],
            "is_blocked": pend["is_blocked"],
            "custom_discount_pct": pend.get("custom_discount_pct"),
            "first_seen": None,
            "ref_count": 0,
            "pending_usd": 0,
            "percent": pct,
            "subscription": sub,
            "days_left": None,
            "_assigned_only": True,
        }
    return {
        "telegram_id": 0,
        "username": username,
        "referred_by": None,
        "referrer": None,
        "is_partner": False,
        "is_gift": False,
        "is_blocked": False,
        "custom_discount_pct": None,
        "first_seen": None,
        "ref_count": 0,
        "pending_usd": 0,
        "percent": 10,
        "subscription": sub,
        "days_left": None,
        "_assigned_only": True,
    }


def list_paid_users() -> list:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_telegram_id FROM payments")
        return [r[0] for r in cur.fetchall()]


def list_recent_payments(limit: int = 30) -> list:
    """Последние платежи для админ-логов (user_id, amount, days, system, order_id, created)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_telegram_id, amount_usd, plan_days, payment_system, merchant_order_id, created_at
            FROM payments ORDER BY id DESC LIMIT ?
        """, (limit,))
        return [
            {"user_id": r[0], "amount": r[1], "days": r[2], "system": r[3] or "manual", "order_id": r[4], "created": r[5]}
            for r in cur.fetchall()
        ]


# --- Settings ---

def get_setting(key: str, default: str = "") -> str:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row and row[0] else default


def set_setting(key: str, value: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (key, value))


def get_free_codes(limit: int = 20) -> list:
    """Коды без привязки (assigned_username пуст), свободные для выдачи."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.code, c.days, c.is_developer
            FROM codes c
            WHERE (c.assigned_username IS NULL OR c.assigned_username = '')
            AND NOT EXISTS (SELECT 1 FROM activations a WHERE a.code_id = c.id AND a.revoked = 0)
            ORDER BY c.id DESC
            LIMIT ?
        """, (limit,))
        return [{"code": r[0], "days": r[1], "is_developer": bool(r[2])} for r in cur.fetchall()]


def get_user_subscription_info(user_id: int, username: str | None = None) -> dict | None:
    """Информация о подписке: код (присвоенный или активированный), срок, статус."""
    un = (username or "").strip().lstrip("@").lower()
    with get_db() as conn:
        cur = conn.cursor()
        # Сначала ищем по активации (user_telegram_id)
        cur.execute("""
            SELECT c.code, c.days, c.is_developer, a.expires_at, a.revoked
            FROM activations a JOIN codes c ON c.id = a.code_id
            WHERE a.user_telegram_id = ? AND a.revoked = 0
            ORDER BY a.activated_at DESC LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        if row:
            return {"code": row[0], "days": row[1], "is_developer": bool(row[2]), "expires_at": row[3], "revoked": bool(row[4]), "status": "activated"}
        # Иначе — по assigned_username (код выдан, но не активирован)
        if un:
            cur.execute("""
                SELECT c.code, c.days, c.is_developer
                FROM codes c
                WHERE LOWER(REPLACE(COALESCE(c.assigned_username,''), '@', '')) = ?
                AND NOT EXISTS (SELECT 1 FROM activations a WHERE a.code_id = c.id AND a.revoked = 0)
                ORDER BY c.id DESC LIMIT 1
            """, (un,))
            row = cur.fetchone()
            if row:
                return {"code": row[0], "days": row[1], "is_developer": bool(row[2]), "expires_at": None, "revoked": False, "status": "assigned"}
    return None


def list_codes_and_activations() -> list:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.code, c.days, c.is_developer, c.assigned_username, c.created_at,
                   a.hwid, a.user_telegram_id, a.activated_at, a.expires_at, a.revoked
            FROM codes c LEFT JOIN activations a ON a.code_id = c.id
            ORDER BY c.created_at DESC
        """)
        rows = cur.fetchall()
    result = []
    seen = set()
    for r in rows:
        if r[0] in seen:
            continue
        seen.add(r[0])
        result.append({
            "code": r[0], "days": r[1], "is_developer": bool(r[2]), "assigned_username": r[3], "created_at": r[4],
            "hwid": r[5], "user_telegram_id": r[6], "activated_at": r[7], "expires_at": r[8], "revoked": bool(r[9]) if r[9] is not None else False,
        })
    return result
