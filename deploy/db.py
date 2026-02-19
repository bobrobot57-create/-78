# -*- coding: utf-8 -*-
"""Схема БД: коды, активации, HWID."""
import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "voicer_licenses.db")


def _get_conn():
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


def init_db():
    with get_db() as conn:
        cur = conn.cursor()
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
        try:
            cur.execute("ALTER TABLE codes ADD COLUMN assigned_username TEXT")
        except sqlite3.OperationalError:
            pass
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
        try:
            cur.execute("ALTER TABLE activations ADD COLUMN installation_id TEXT")
        except sqlite3.OperationalError:
            pass
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
        conn.commit()


def get_owner_id():
    ids = os.environ.get("ADMIN_USER_IDS", "").strip()
    if not ids:
        return None
    try:
        return int(ids.split(",")[0].strip())
    except ValueError:
        return None


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
