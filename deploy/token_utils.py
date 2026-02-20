# -*- coding: utf-8 -*-
"""Создание токена активации для офлайн-проверки (через Telegram)."""
import base64
import hashlib
import hmac
import json
import os
from datetime import datetime

from db import activate_code

API_SECRET = os.environ.get("API_SECRET", "")


def create_activation_token(code: str, hwid: str, installation_id: str = "") -> tuple[bool, str]:
    """
    Активирует код и создаёт токен. Возвращает (ok, token_or_error).
    Токен жив 15 мин. Формат: base64(json).hmac_hex
    """
    if not API_SECRET:
        return False, "Сервер не настроен (API_SECRET)."
    code = (code or "").strip().upper()
    hwid = (hwid or "").strip()
    installation_id = (installation_id or "").strip() or None
    if not code or not hwid:
        return False, "Нужны код и HWID."
    result = activate_code(code, hwid, installation_id)
    if not result.get("ok"):
        err = result.get("error", "unknown")
        msg = {"invalid_code": "Неверный код", "expired": "Срок истёк",
               "revoked": "Код отозван", "code_already_used": "Код уже использован"}.get(err, err)
        return False, msg
    payload = {
        "c": code,
        "h": hwid,
        "i": installation_id or "",
        "e": result.get("expires_at"),
        "d": result.get("is_developer", False),
        "t": int(datetime.utcnow().timestamp()),
    }
    j = json.dumps(payload, sort_keys=True)
    sig = hmac.new(API_SECRET.encode(), j.encode(), hashlib.sha256).hexdigest()
    token = base64.b64encode(j.encode()).decode() + "." + sig
    return True, token
