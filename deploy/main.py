# -*- coding: utf-8 -*-
"""
VoiceLab License Server — webhook-режим.
Сервер спит, просыпается только на запрос: бот, API, активация exe.
"""
import logging
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

from db import init_db, check_license, activate_code
from handlers import build_admin_app, build_client_app, set_client_bot


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
    result = check_license(code, hwid, installation_id)
    if result["ok"]:
        return JSONResponse({"ok": True, "expires_at": result["expires_at"], "is_developer": result["is_developer"]})
    if result["error"] == "not_activated":
        act = activate_code(code, hwid, installation_id)
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


async def run():
    global admin_app, client_app
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
