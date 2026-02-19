# -*- coding: utf-8 -*-
"""Обработчики ботов — общая логика для админ и клиент."""
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TimedOut, NetworkError
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler,
    filters,
)
from db import (
    create_code, create_codes_batch, revoke_code, list_codes_and_activations,
    get_owner_id, add_admin, remove_admin, list_admins, is_appointed_admin,
    set_code_assigned, delete_code, delete_all_codes,
)


def _is_owner(user_id: int) -> bool:
    return get_owner_id() is not None and user_id == get_owner_id()


def _is_admin(user_id: int) -> bool:
    return _is_owner(user_id) or is_appointed_admin(user_id)


def _main_menu_keyboard(is_owner: bool):
    kb = [
        [InlineKeyboardButton("💰 Создать код", callback_data="create_code_menu")],
        [InlineKeyboardButton("📋 Список кодов", callback_data="list_codes")],
    ]
    if is_owner:
        kb.append([InlineKeyboardButton("👥 Админы", callback_data="list_admins")])
    return InlineKeyboardMarkup(kb)


def _create_code_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("30 дней", callback_data="code_30"), InlineKeyboardButton("90 дней", callback_data="code_90")],
        [InlineKeyboardButton("♾ Вечный", callback_data="code_dev_1")],
        [InlineKeyboardButton("◀️ Назад", callback_data="main_menu")],
    ])


def _back_to_menu_keyboard(is_owner: bool):
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ В меню", callback_data="main_menu")]])


CODES_LEGEND = "_код | тип | @user | ст. | срок_\n\n"


def _build_codes_list(rows: list, page: int, total_pages: int, search: str, context) -> tuple:
    from datetime import datetime
    now = datetime.utcnow()
    PAGE_SIZE = 10
    start = page * PAGE_SIZE
    page_rows = rows[start:start + PAGE_SIZE]
    kb, lines = [], []
    for r in page_rows:
        dev = "DEV" if r["is_developer"] else f"{r['days']}д"
        acc = f"@{r['assigned_username']}" if r.get("assigned_username") else "—"
        status = "отозван" if r.get("revoked") else ("акт" if r.get("hwid") else "—")
        exp_raw = r.get("expires_at")
        days_str = "∞" if not exp_raw or r["is_developer"] else (f"{max(0, (datetime.fromisoformat(exp_raw) - now).days)}д" if exp_raw else "?")
        rev = " ❌" if r.get("revoked") else ""
        lines.append(f"`{r['code']}` {dev} {acc} {status} {days_str}{rev}")
        kb.append([InlineKeyboardButton("🔗", callback_data=f"a_{r['code']}"), InlineKeyboardButton("🗑", callback_data=f"d_{r['code']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"list_codes:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"list_codes:{page+1}"))
    kb.append(nav)
    footer = [InlineKeyboardButton("🔍 Поиск", callback_data="code_search"), InlineKeyboardButton("🔄", callback_data="list_codes")]
    if search:
        footer.insert(1, InlineKeyboardButton("✖", callback_data="code_search_clear"))
    footer.extend([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
    kb.append(footer)
    kb.append([InlineKeyboardButton("🗑 Удалить ВСЕ", callback_data="del_all_confirm")])
    return lines, kb


def _admins_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить админа", callback_data="add_admin")],
        [InlineKeyboardButton("◀️ В меню", callback_data="main_menu")],
    ])


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "too old" in str(e).lower() or "invalid" in str(e).lower():
            return
        raise
    except (TimedOut, NetworkError):
        return
    user_id = update.effective_user.id
    if not _is_admin(user_id):
        await query.edit_message_text("⛔ Доступ запрещён.")
        return
    data = query.data
    is_owner = _is_owner(user_id)

    if data == "main_menu":
        role = "👑 Владелец" if is_owner else "👤 Админ"
        await query.edit_message_text(f"🎛 *Панель VoiceLab*\n\nВаша роль: {role}\n\nВыберите действие:", parse_mode="Markdown", reply_markup=_main_menu_keyboard(is_owner))
        return
    if data == "create_code_menu":
        await query.edit_message_text("💰 *Создать код*\n\nВыберите тип:", parse_mode="Markdown", reply_markup=_create_code_keyboard())
        return
    if data == "code_30":
        code = create_code(days=30)
        await query.edit_message_text(f"✅ *Код на 30 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "code_90":
        code = create_code(days=90)
        await query.edit_message_text(f"✅ *Код на 90 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "code_dev_1":
        code = create_code(days=0, is_developer=True)
        await query.edit_message_text(f"✅ *Вечный код*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "list_codes" or (data.startswith("list_codes:") and len(data) > 11):
        page = int(data.split(":")[1]) if data.startswith("list_codes:") else 0
        search = context.user_data.get("code_search") or ""
        rows = list_codes_and_activations()
        if search:
            rows = [r for r in rows if r.get("assigned_username") and search.lower() in (r["assigned_username"] or "").lower()]
        if not rows:
            await query.edit_message_text("📭 Нет кодов." + (f"\nПоиск: @{search}" if search else ""), reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Поиск", callback_data="code_search")],
                [InlineKeyboardButton("🔄 Обновить", callback_data="list_codes")],
                [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
            ]))
        else:
            total_pages = max(1, (len(rows) + 9) // 10)
            page = max(0, min(page, total_pages - 1))
            lines, kb = _build_codes_list(rows, page, total_pages, search, context)
            header = f"Поиск: @{search}\n\n" if search else ""
            await query.edit_message_text(f"📋 *Коды* ({len(rows)})\n{CODES_LEGEND}{header}" + "\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "code_search":
        context.user_data["awaiting_code_search"] = True
        context.user_data["_list_msg"] = (query.message.chat_id, query.message.message_id)
        await query.edit_message_text("🔍 *Поиск по @username*\n\nОтправьте @username:", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="list_codes")]]))
        return
    if data == "code_search_clear":
        context.user_data.pop("code_search", None)
        context.user_data.pop("awaiting_code_search", None)
        rows = list_codes_and_activations()
        lines, kb = _build_codes_list(rows, 0, max(1, (len(rows) + 9) // 10), "", context) if rows else ([], [])
        await query.edit_message_text("📋 *Коды*\n" + ("\n".join(lines) if lines else "📭 Нет кодов."), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb) if kb else InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
        return
    if data == "del_all_confirm":
        n = len(list_codes_and_activations())
        await query.edit_message_text(f"🗑 Удалить ВСЕ {n} кодов?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data="del_all_ok"), InlineKeyboardButton("❌ Нет", callback_data="list_codes")],
        ]))
        return
    if data == "del_all_ok":
        n = delete_all_codes()
        context.user_data.pop("code_search", None)
        await query.edit_message_text(f"✅ Удалено: {n}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
        return
    if data.startswith("a_") and len(data) > 2:
        context.user_data["awaiting_assign_for"] = data[2:]
        await query.edit_message_text(f"🔗 Привязать код. Отправьте @username:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="list_codes")]]))
        return
    if data.startswith("d_") and len(data) > 2:
        await query.edit_message_text(f"🗑 Удалить код `{data[2:]}`?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data=f"del_ok_{data[2:]}"), InlineKeyboardButton("❌ Нет", callback_data="list_codes")],
        ]))
        return
    if data.startswith("del_ok_") and len(data) > 7:
        delete_code(data[7:])
        rows = list_codes_and_activations()
        if not rows:
            await query.edit_message_text("📭 Кодов не осталось.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
        else:
            lines, kb = _build_codes_list(rows, 0, max(1, (len(rows) + 9) // 10), "", context)
            await query.edit_message_text("📋 *Коды*\n" + "\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "list_admins" and is_owner:
        owner_id = get_owner_id()
        admins = list_admins()
        lines = [f"👑 Владелец: `{owner_id}`"] + [f"👤 `{a['telegram_id']}`" for a in admins]
        await query.edit_message_text("👥 *Админы*\n\n" + "\n".join(lines), parse_mode="Markdown", reply_markup=_admins_keyboard())
        return
    if data == "noop":
        return
    if data == "add_admin" and is_owner:
        context.user_data["awaiting_admin_id"] = True
        await query.edit_message_text("➕ Отправьте ID пользователя (у @userinfobot):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="main_menu")]]))
        return


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    role = "👑 Владелец" if _is_owner(update.effective_user.id) else "👤 Админ"
    await update.message.reply_text(f"🎛 *Панель VoiceLab*\n\nРоль: {role}", parse_mode="Markdown", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))


async def cmd_newcode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    days = int(context.args[0]) if context.args and str(context.args[0]).isdigit() else 30
    days = max(1, min(365, days))
    code = create_code(days=days)
    await update.message.reply_text(f"✅ Код: `{code}`", parse_mode="Markdown")


async def cmd_devcode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    count = min(20, max(1, int(context.args[0]) if context.args and str(context.args[0]).isdigit() else 1))
    codes = create_codes_batch(count=count, is_developer=True)
    await update.message.reply_text("✅ " + "\n".join(f"`{c}`" for c in codes), parse_mode="Markdown")


async def cmd_codes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    rows = list_codes_and_activations()
    if not rows:
        await update.message.reply_text("📭 Нет кодов.")
        return
    lines = [f"{r['code']} | {r.get('hwid') or '—'}" for r in rows[:40]]
    await update.message.reply_text("📋 Коды:\n" + "\n".join(lines))


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id) or not context.args:
        return
    code = context.args[0].strip().upper()
    if revoke_code(code):
        await update.message.reply_text(f"✅ `{code}` отозван.", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Код не найден.")


async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args and str(context.args[0]).strip().isdigit():
        target_id = int(context.args[0].strip())
    if target_id is None:
        await update.message.reply_text("Использование: /addadmin 123456789 или ответ на сообщение")
        return
    if target_id == get_owner_id():
        await update.message.reply_text("⚠️ Владелец уже в системе.")
        return
    add_admin(target_id, None, update.effective_user.id)
    await update.message.reply_text(f"✅ {target_id} добавлен.")


async def cmd_removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id) or not context.args:
        return
    try:
        target_id = int(context.args[0].strip())
    except ValueError:
        return
    if target_id == get_owner_id():
        await update.message.reply_text("⚠️ Владельца нельзя удалить.")
        return
    if remove_admin(target_id):
        await update.message.reply_text(f"✅ {target_id} убран.")
    else:
        await update.message.reply_text("❌ Не в списке.")


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update.effective_user.id):
        return
    owner_id = get_owner_id()
    admins = list_admins()
    lines = [f"👑 Владелец: {owner_id}"] + [f"👤 {a['telegram_id']}" for a in admins]
    await update.message.reply_text("📋 Админы:\n" + "\n".join(lines))


async def on_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(update.effective_user.id):
        return
    text = (update.message.text or "").strip().lower()

    if context.user_data.get("awaiting_code_search"):
        context.user_data.pop("awaiting_code_search", None)
        if text in ("отмена", "cancel"):
            context.user_data.pop("code_search", None)
            await update.message.reply_text("Отменено.")
            return
        context.user_data["code_search"] = text.lstrip("@")
        await update.message.reply_text(f"Поиск: @{context.user_data['code_search']}. Нажмите «Список кодов» в меню.")
        return

    if context.user_data.get("awaiting_assign_for"):
        code_val = context.user_data.pop("awaiting_assign_for", None)
        if text in ("отмена", "cancel"):
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
            return
        if code_val and set_code_assigned(code_val, text):
            await update.message.reply_text(f"✅ Привязано к @{text.lstrip('@')}")
        await update.message.reply_text("🎛 Меню:", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
        return

    if context.user_data.get("awaiting_admin_id") and _is_owner(update.effective_user.id):
        if text in ("отмена", "cancel"):
            context.user_data.pop("awaiting_admin_id", None)
            return
        if text.isdigit():
            target_id = int(text)
            context.user_data.pop("awaiting_admin_id", None)
            if target_id != get_owner_id():
                add_admin(target_id, None, update.effective_user.id)
                await update.message.reply_text(f"✅ {target_id} добавлен.")
        return


def _client_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Купить", callback_data="client_buy")],
        [InlineKeyboardButton("🔑 Мой код", callback_data="client_mycode")],
    ])


async def client_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except (BadRequest, TimedOut, NetworkError):
        return
    user_id = update.effective_user.id
    if query.data == "client_buy":
        await query.edit_message_text("Напишите администратору для оплаты.")
        return
    if query.data == "client_mycode":
        rows = list_codes_and_activations()
        my = [r for r in rows if r.get("user_telegram_id") == user_id and not r.get("revoked")]
        if not my:
            await query.edit_message_text("У вас нет кода.")
        else:
            r = my[0]
            await query.edit_message_text(f"Код: `{r['code']}`\nДо: {r.get('expires_at') or 'бессрочно'}", parse_mode="Markdown")


async def client_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».",
        parse_mode="Markdown",
        reply_markup=_client_keyboard()
    )


async def client_mycode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rows = list_codes_and_activations()
    my = [r for r in rows if r.get("user_telegram_id") == user_id and not r.get("revoked")]
    if not my:
        await update.message.reply_text("У вас нет кода.")
    else:
        r = my[0]
        await update.message.reply_text(f"Код: `{r['code']}`", parse_mode="Markdown")


async def client_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").lower()
    if "оплатил" in text or "купить" in text:
        await update.message.reply_text("Напишите администратору. После подтверждения получите код.")


async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, (TimedOut, NetworkError)):
        return
    raise context.error


def build_admin_app(token: str) -> Application:
    app = Application.builder().token(token).updater(None).build()
    app.add_error_handler(_error_handler)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("newcode", cmd_newcode))
    app.add_handler(CommandHandler("devcode", cmd_devcode))
    app.add_handler(CommandHandler("codes", cmd_codes))
    app.add_handler(CommandHandler("revoke", cmd_revoke))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CommandHandler("removeadmin", cmd_removeadmin))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_admin_input))
    return app


def build_client_app(token: str) -> Application:
    app = Application.builder().token(token).updater(None).build()
    app.add_error_handler(_error_handler)
    app.add_handler(CommandHandler("start", client_start))
    app.add_handler(CommandHandler("mycode", client_mycode))
    app.add_handler(CallbackQueryHandler(client_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, client_buy))
    return app
