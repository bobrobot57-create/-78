# -*- coding: utf-8 -*-
"""
Интеграция платёжных систем: FreeKassa и Cryptomus.
Генерация ссылок на оплату и обработка webhook.
"""
import hashlib
import json
import os
import time
import urllib.parse
import base64
import logging

import httpx

from db import get_setting

log = logging.getLogger(__name__)


def _get(key: str, default: str = "") -> str:
    """Читает настройку: сначала env (UPPER), потом settings в БД."""
    env_key = key.upper().replace(".", "_")
    return (os.environ.get(env_key) or "").strip() or get_setting(key, default)


# --- FreeKassa ---

def generate_freekassa_link(user_id: int, amount: float, plan_days: int) -> str | None:
    """
    Генерирует ссылку на оплату FreeKassa.
    Возвращает URL или None, если не настроено.
    """
    merchant_id = _get("fk_merchant_id")
    secret1 = _get("fk_secret_1")
    if not merchant_id or not secret1:
        return None
    currency = _get("fk_currency", "USD") or "USD"
    order_id = f"{user_id}_{plan_days}_{int(time.time())}"
    sign_string = f"{merchant_id}:{amount}:{secret1}:{currency}:{order_id}"
    sign = hashlib.md5(sign_string.encode("utf-8")).hexdigest()
    params = {
        "m": merchant_id,
        "oa": amount,
        "o": order_id,
        "s": sign,
        "currency": currency,
        "us_userid": user_id,
        "us_days": plan_days,
    }
    return "https://pay.freekassa.ru/?" + urllib.parse.urlencode(params)


def verify_freekassa_webhook(merchant_id: str, amount: str, order_id: str, sign_received: str) -> bool:
    """Проверка подписи webhook FreeKassa. Формула: md5(merchant_id:amount:secret2:order_id)."""
    secret2 = _get("fk_secret_2")
    if not secret2:
        return False
    sign_string = f"{merchant_id}:{amount}:{secret2}:{order_id}"
    sign_calc = hashlib.md5(sign_string.encode("utf-8")).hexdigest()
    return sign_calc == sign_received


# --- Cryptomus ---

def create_cryptomus_invoice(amount: float, order_id: str, user_id: int, plan_days: int,
                             url_callback: str) -> dict | None:
    """
    Создаёт инвойс в Cryptomus. Возвращает {"url": "...", "uuid": "..."} или None при ошибке.
    """
    merchant = _get("cryptomus_merchant")
    api_key = _get("cryptomus_api_key")
    if not merchant or not api_key:
        return None
    body = {
        "amount": str(amount),
        "currency": "USD",
        "order_id": order_id,
        "url_callback": url_callback,
        "additional_data": json.dumps({"user_id": user_id, "days": plan_days})[:255],
    }
    body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    sign = hashlib.md5((base64.b64encode(body_json.encode("utf-8")).decode() + api_key).encode()).hexdigest()
    headers = {
        "merchant": merchant,
        "sign": sign,
        "Content-Type": "application/json",
    }
    try:
        with httpx.Client(timeout=15) as client:
            r = client.post("https://api.cryptomus.com/v1/payment", content=body_json, headers=headers)
            data = r.json()
            if data.get("state") == 0 and data.get("result"):
                res = data["result"]
                return {"url": res.get("url"), "uuid": res.get("uuid")}
            log.warning("Cryptomus create invoice error: %s", data)
            return None
    except Exception as e:
        log.error("Cryptomus request failed: %s", e)
        return None


def verify_cryptomus_webhook(body: dict, sign_received: str) -> bool:
    """Проверка подписи webhook Cryptomus: MD5(base64(json без sign) + api_key)."""
    api_key = _get("cryptomus_api_key")
    if not api_key:
        return False
    data_copy = {k: v for k, v in body.items() if k != "sign"}
    body_json = json.dumps(data_copy, separators=(",", ":"), ensure_ascii=False)
    sign_calc = hashlib.md5((base64.b64encode(body_json.encode("utf-8")).decode() + api_key).encode()).hexdigest()
    return sign_calc == sign_received
