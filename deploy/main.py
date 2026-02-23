# -*- coding: utf-8 -*-
"""
VoiceLab License Server — webhook-режим.
Сервер спит, просыпается только на запрос: бот, API, активация exe.
"""
import logging
import traceback

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

import os
import asyncio
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from telegram import Update, BotCommand

from db import init_db, check_license, activate_code, create_code, add_payment, payment_exists_by_order_id, get_all_admin_ids, list_admins, get_user
from handlers import build_admin_app, build_client_app, set_client_bot, get_client_bot
from payment import (
    generate_freekassa_link,
    verify_freekassa_webhook,
    create_cryptomus_invoice,
    verify_cryptomus_webhook,
)


ADMIN_TOKEN = os.environ.get("ADMIN_BOT_TOKEN", "")
CLIENT_TOKEN = os.environ.get("CLIENT_BOT_TOKEN", "")
WEBHOOK_BASE = os.environ.get("WEBHOOK_BASE_URL", "").rstrip("/")
API_SECRET = os.environ.get("API_SECRET", "")


def _check_secret(request: Request) -> bool:
    secret = request.headers.get("X-API-Secret", "")
    if API_SECRET and secret != API_SECRET:
        return False
    return True


async def api_check(request: Request):
    """POST /check — проверка/активация лицензии."""
    if not _check_secret(request):
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)
    code = (data.get("code") or "").strip().upper()
    hwid = (data.get("hwid") or "").strip()
    installation_id = (data.get("installation_id") or "").strip() or None
    if not code or not hwid:
        return JSONResponse({"ok": False, "error": "missing_code_or_hwid"}, status_code=400)
    try:
        result = await asyncio.to_thread(check_license, code, hwid, installation_id)
    except Exception as e:
        log.error("check_license: %s\n%s", e, traceback.format_exc())
        return JSONResponse({"ok": False, "error": "server_error"}, status_code=500)
    if result["ok"]:
        return JSONResponse({"ok": True, "expires_at": result["expires_at"], "is_developer": result["is_developer"]})
    if result["error"] == "not_activated":
        try:
            act = await asyncio.to_thread(activate_code, code, hwid, installation_id)
        except Exception as e:
            log.error("activate_code: %s\n%s", e, traceback.format_exc())
            return JSONResponse({"ok": False, "error": "server_error"}, status_code=500)
        if act["ok"]:
            return JSONResponse({"ok": True, "expires_at": act["expires_at"], "is_developer": act.get("is_developer", False)})
        return JSONResponse({"ok": False, "error": act.get("error", "activation_failed")}, status_code=400)
    return JSONResponse({"ok": False, "error": result["error"]}, status_code=400)


async def health(request: Request):
    return JSONResponse({"status": "ok"})


async def webhook_admin(request: Request):
    if not ADMIN_TOKEN:
        return Response(status_code=500)
    try:
        data = await request.json()
    except Exception:
        return Response(status_code=400)
    update = Update.de_json(data, admin_app.bot)
    await admin_app.update_queue.put(update)
    return Response()


async def payment_freekassa(request: Request):
    """Webhook FreeKassa: проверка подписи, создание кода, отправка пользователю."""
    try:
        form = await request.form()
    except Exception:
        return Response("Error: invalid form", status_code=400)
    merchant_id = form.get("MERCHANT_ID")
    amount = form.get("AMOUNT")
    order_id = form.get("MERCHANT_ORDER_ID")
    sign_received = form.get("SIGN")
    user_id = form.get("us_userid")
    days = form.get("us_days")
    if not all([merchant_id, amount, order_id, sign_received, user_id, days]):
        log.warning("FreeKassa webhook: missing params")
        return Response("Error: missing parameters", status_code=400)
    if not verify_freekassa_webhook(merchant_id, amount, order_id, sign_received):
        log.warning("FreeKassa webhook: bad sign")
        return Response("Error: bad sign", status_code=403)
    if await asyncio.to_thread(payment_exists_by_order_id, order_id):
        log.info("FreeKassa webhook: duplicate order_id %s", order_id)
        return Response("YES", status_code=200)
    try:
        user_id = int(user_id)
        days = int(days)
        amount_float = float(amount)
    except (ValueError, TypeError):
        return Response("Error: invalid params", status_code=400)
    try:
        new_code = await asyncio.to_thread(create_code, days=days)
        await asyncio.to_thread(add_payment, user_telegram_id=user_id, amount_usd=amount_float, plan_days=int(days),
                               merchant_order_id=order_id, payment_system="freekassa")
        bot = get_client_bot()
        if bot:
            msg = f"✅ *Оплата получена!*\n\nВаш ключ на {days} дней:\n`{new_code}`\n\nСкопируйте его и вставьте в программу VoiceLab."
            asyncio.create_task(bot.send_message(chat_id=user_id, text=msg, parse_mode="Markdown"))
        asyncio.create_task(_notify_admin_payment(user_id, amount_float, days, "freekassa", new_code))
        log.info("FreeKassa: payment ok order_id=%s user=%s days=%s", order_id, user_id, days)
        return Response("YES", status_code=200)
    except Exception as e:
        log.error("FreeKassa webhook error: %s\n%s", e, traceback.format_exc())
        return Response("Error processing", status_code=500)


async def payment_cryptomus(request: Request):
    """Webhook Cryptomus: проверка подписи, создание кода, отправка пользователю."""
    try:
        body = await request.json()
    except Exception:
        return Response("Error: invalid json", status_code=400)
    sign_received = body.get("sign")
    if not sign_received:
        return Response("Error: no sign", status_code=400)
    if not verify_cryptomus_webhook(body, sign_received):
        log.warning("Cryptomus webhook: bad sign")
        return Response("Error: bad sign", status_code=403)
    status = body.get("status")
    if status not in ("paid", "paid_over"):
        return Response("OK", status_code=200)
    order_id = body.get("order_id")
    if not order_id:
        return Response("Error: no order_id", status_code=400)
    if await asyncio.to_thread(payment_exists_by_order_id, order_id):
        log.info("Cryptomus webhook: duplicate order_id %s", order_id)
        return Response("OK", status_code=200)
    add_data = body.get("additional_data")
    if add_data:
        try:
            add = json.loads(add_data) if isinstance(add_data, str) else add_data
            user_id = add.get("user_id")
            days = add.get("days")
        except (json.JSONDecodeError, TypeError):
            user_id = days = None
    else:
        user_id = days = None
    if not user_id or not days:
        log.warning("Cryptomus webhook: no user_id/days in additional_data")
        return Response("Error: missing user_id/days", status_code=400)
    try:
        amount_float = float(body.get("amount") or body.get("payment_amount_usd") or 0)
    except (ValueError, TypeError):
        amount_float = 0
    try:
        new_code = await asyncio.to_thread(create_code, days=int(days))
        await asyncio.to_thread(add_payment, user_telegram_id=int(user_id), amount_usd=amount_float, plan_days=int(days),
                               merchant_order_id=order_id, payment_system="cryptomus")
        bot = get_client_bot()
        if bot:
            msg = f"✅ *Оплата получена!*\n\nВаш ключ на {days} дней:\n`{new_code}`\n\nСкопируйте его и вставьте в программу VoiceLab."
            asyncio.create_task(bot.send_message(chat_id=int(user_id), text=msg, parse_mode="Markdown"))
        asyncio.create_task(_notify_admin_payment(int(user_id), amount_float, int(days), "cryptomus", new_code))
        log.info("Cryptomus: payment ok order_id=%s user=%s days=%s", order_id, user_id, days)
        return Response("OK", status_code=200)
    except Exception as e:
        log.error("Cryptomus webhook error: %s\n%s", e, traceback.format_exc())
        return Response("Error processing", status_code=500)


async def webhook_client(request: Request):
    if not CLIENT_TOKEN:
        return Response(status_code=500)
    try:
        data = await request.json()
    except Exception:
        return Response(status_code=400)
    update = Update.de_json(data, client_app.bot)
    await client_app.update_queue.put(update)
    return Response()


# Глобальные приложения (инициализируются в run)
admin_app = None
client_app = None


async def _notify_admin_payment(user_id: int, amount: float, days: int, system: str, new_code: str):
    """Отправляет уведомление о платеже владельцу и админам в админ-бот."""
    if not admin_app or not admin_app.bot:
        return
    u = get_user(user_id)
    un = (u.get("username") or "").strip().lstrip("@")
    username = f"@{un}" if un else f"ID:{user_id}"
    sys_icon = "💳" if system == "freekassa" else "₿" if system == "cryptomus" else "💰"
    msg = (
        f"{sys_icon} *Оплата получена*\n\n"
        f"👤 {username} (`{user_id}`)\n"
        f"💵 ${amount} · {days} дней\n"
        f"🔑 Код: `{new_code}`\n"
        f"📦 {system}"
    )
    chat_ids = set(get_all_admin_ids())
    for a in list_admins():
        chat_ids.add(a["telegram_id"])
    for cid in chat_ids:
        try:
            await admin_app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown")
        except Exception as e:
            log.warning("Notify admin payment to %s: %s", cid, e)


async def run():
    global admin_app, client_app
    # Пул потоков для asyncio.to_thread
    import concurrent.futures
    pool_size = int(os.environ.get("THREAD_POOL_SIZE", "32"))
    asyncio.get_event_loop().set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=pool_size))
    init_db()

    if not WEBHOOK_BASE:
        print("⚠️ WEBHOOK_BASE_URL не задан! Задай в переменных окружения (например https://твой-проект.up.railway.app)")
    if not ADMIN_TOKEN:
        print("⚠️ ADMIN_BOT_TOKEN не задан")
    if not CLIENT_TOKEN:
        print("⚠️ CLIENT_BOT_TOKEN не задан")

    admin_app = build_admin_app(ADMIN_TOKEN) if ADMIN_TOKEN else None
    client_app = build_client_app(CLIENT_TOKEN) if CLIENT_TOKEN else None
    if client_app:
        set_client_bot(client_app.bot)

    if admin_app:
        await admin_app.initialize()
        if WEBHOOK_BASE:
            try:
                await admin_app.bot.set_webhook(f"{WEBHOOK_BASE}/webhook/admin")
            except Exception as e:
                print(f"⚠️ Webhook admin: {e}. Проверь WEBHOOK_BASE_URL и DNS.")
    if client_app:
        await client_app.initialize()
        await client_app.bot.set_my_commands([
            BotCommand("start", "Главное меню"),
            BotCommand("mycode", "Мой код активации"),
        ])
        if WEBHOOK_BASE:
            try:
                await client_app.bot.set_webhook(f"{WEBHOOK_BASE}/webhook/client")
            except Exception as e:
                print(f"⚠️ Webhook client: {e}. Проверь WEBHOOK_BASE_URL и DNS.")

    routes = [
        Route("/check", api_check, methods=["POST"]),
        Route("/health", health, methods=["GET"]),
        Route("/payment/freekassa", payment_freekassa, methods=["POST"]),
        Route("/payment/cryptomus", payment_cryptomus, methods=["POST"]),
    ]
    if admin_app:
        routes.append(Route("/webhook/admin", webhook_admin, methods=["POST"]))
    if client_app:
        routes.append(Route("/webhook/client", webhook_client, methods=["POST"]))

    app = Starlette(routes=routes)

    port = int(os.environ.get("PORT", 5000))
    import uvicorn
    config = uvicorn.Config(app, host="0.0.0.0", port=port)

    # Процессоры ботов + веб-сервер — все параллельно
    server = uvicorn.Server(config)
    tasks = [server.serve()]
    if admin_app:
        tasks.append(admin_app.start())
    if client_app:
        tasks.append(client_app.start())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(run())
