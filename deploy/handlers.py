# -*- coding: utf-8 -*-
"""Обработчики ботов — общая логика для админ и клиент."""
import os

# Клиентский бот для рассылок (устанавливается из main.py)
_client_bot = None

def set_client_bot(bot):
    global _client_bot
    _client_bot = bot
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
    ensure_user, get_user, get_user_by_username, set_partner, set_custom_discount,
    list_referrals, add_payment, get_referral_stats, get_user_payouts, get_user_total_pending,
    list_all_users, list_paid_users, get_setting, set_setting,
)


def _is_owner(user_id: int) -> bool:
    return get_owner_id() is not None and user_id == get_owner_id()


def _is_admin(user_id: int) -> bool:
    return _is_owner(user_id) or is_appointed_admin(user_id)


def _main_menu_keyboard(is_owner: bool):
    kb = [
        [InlineKeyboardButton("💰 Создать код", callback_data="create_code_menu")],
        [InlineKeyboardButton("📋 Список кодов", callback_data="list_codes")],
        [InlineKeyboardButton("📊 Рефералы", callback_data="ref_stats")],
    ]
    if is_owner:
        kb.append([InlineKeyboardButton("👥 Админы", callback_data="list_admins")])
        kb.append([InlineKeyboardButton("⚙️ Настройки", callback_data="settings_menu")])
        kb.append([InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_menu")])
    return InlineKeyboardMarkup(kb)


def _create_code_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("30 дней", callback_data="code_30"), InlineKeyboardButton("60 дней", callback_data="code_60"), InlineKeyboardButton("90 дней", callback_data="code_90")],
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
    if data == "code_60":
        code = create_code(days=60)
        await query.edit_message_text(f"✅ *Код на 60 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
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
    if data == "ref_stats":
        context.user_data.pop("awaiting_payment", None)
        context.user_data.pop("awaiting_set_partner", None)
        context.user_data.pop("awaiting_set_discount", None)
        stats = get_referral_stats()
        if not stats:
            await query.edit_message_text("📊 *Рефералы*\n\nПока нет рефералов.", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
            return
        lines = []
        for s in stats:
            role = "🤝 Партнёр" if s["is_partner"] else "👤 Клиент"
            pct = s["percent"]
            un = f"@{s['username']}" if s.get("username") else f"ID:{s['telegram_id']}"
            lines.append(f"{role} {un}\n  Рефералов: {s['ref_count']} | Ставка: {pct}% | К выплате: ${s['pending_usd']}")
        kb = [
            [InlineKeyboardButton("➕ Записать платёж", callback_data="record_payment")],
            [InlineKeyboardButton("🤝 Назначить партнёра", callback_data="set_partner")],
            [InlineKeyboardButton("✏️ Скидка для реферала", callback_data="set_discount")],
            [InlineKeyboardButton("🔄 Обновить", callback_data="ref_stats")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]
        await query.edit_message_text("📊 *Рефералы*\n\n" + "\n\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "record_payment" and is_owner:
        context.user_data["awaiting_payment"] = "amount"
        await query.edit_message_text("➕ *Записать платёж*\n\nОтправьте: сумма долларов, дни\nНапример: `35 30`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ref_stats")]]))
        return
    if data == "settings_menu" and is_owner:
        welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
        price_30 = get_setting("price_30", "15")
        price_60 = get_setting("price_60", "25")
        price_90 = get_setting("price_90", "35")
        software_url = get_setting("software_url", "https://drive.google.com/")
        text = f"⚙️ *Настройки*\n\nПриветствие: _{welcome[:50]}..._\n\nЦены (USD): 30д={price_30} | 60д={price_60} | 90д={price_90}\nСофт: {software_url[:40]}..."
        kb = [
            [InlineKeyboardButton("✏️ Приветствие", callback_data="set_welcome")],
            [InlineKeyboardButton("💵 Цены", callback_data="set_prices")],
            [InlineKeyboardButton("📥 Ссылка на софт", callback_data="set_software_url")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "broadcast_menu" and is_owner:
        users = list_all_users()
        paid = set(list_paid_users())
        refs = set(u["telegram_id"] for u in get_referral_stats())
        text = f"📢 *Рассылка*\n\nВсего пользователей: {len(users)}\nКупили: {len(paid)}\nРефералы: {len(refs)}"
        kb = [
            [InlineKeyboardButton("📤 Всем", callback_data="broadcast_all")],
            [InlineKeyboardButton("💰 Купившим", callback_data="broadcast_paid")],
            [InlineKeyboardButton("🔗 Рефералам", callback_data="broadcast_refs")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "noop":
        return
    if data == "add_admin" and is_owner:
        context.user_data["awaiting_admin_id"] = True
        await query.edit_message_text("➕ Отправьте ID пользователя (у @userinfobot):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="main_menu")]]))
        return
    if data == "set_welcome" and is_owner:
        context.user_data["awaiting_setting"] = "welcome_message"
        await query.edit_message_text("✏️ Отправьте текст приветствия (Markdown):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]]))
        return
    if data == "set_prices" and is_owner:
        context.user_data["awaiting_setting"] = "prices"
        await query.edit_message_text("💵 Отправьте цены через пробел: 30д 60д 90д\nНапример: 15 25 35", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]]))
        return
    if data == "set_software_url" and is_owner:
        context.user_data["awaiting_setting"] = "software_url"
        await query.edit_message_text("📥 Отправьте ссылку на Google Drive:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]]))
        return
    if data.startswith("broadcast_") and is_owner:
        context.user_data["awaiting_broadcast"] = data.replace("broadcast_", "")
        await query.edit_message_text("📤 Отправьте текст рассылки:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="broadcast_menu")]]))
        return
    if data == "set_partner" and is_owner:
        context.user_data["awaiting_set_partner"] = True
        await query.edit_message_text("🤝 Отправьте @username или ID пользователя для назначения партнёром:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ref_stats")]]))
        return
    if data == "set_discount" and is_owner:
        context.user_data["awaiting_set_discount"] = "user"
        await query.edit_message_text("✏️ Отправьте @username или ID реферала:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ref_stats")]]))
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

    if context.user_data.get("awaiting_setting") and _is_owner(update.effective_user.id):
        key = context.user_data.pop("awaiting_setting", None)
        if text in ("отмена", "cancel"):
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        if key == "welcome_message":
            set_setting("welcome_message", update.message.text)
            await update.message.reply_text("✅ Приветствие обновлено.", reply_markup=_main_menu_keyboard(True))
        elif key == "prices":
            parts = update.message.text.strip().split()
            if len(parts) >= 3:
                set_setting("price_30", parts[0])
                set_setting("price_60", parts[1])
                set_setting("price_90", parts[2])
                await update.message.reply_text("✅ Цены обновлены.", reply_markup=_main_menu_keyboard(True))
            else:
                await update.message.reply_text("⚠️ Нужно 3 числа: 30д 60д 90д")
                context.user_data["awaiting_setting"] = "prices"
        elif key == "software_url":
            set_setting("software_url", update.message.text.strip())
            await update.message.reply_text("✅ Ссылка обновлена.", reply_markup=_main_menu_keyboard(True))
        return

    if context.user_data.get("awaiting_broadcast") and _is_owner(update.effective_user.id):
        target = context.user_data.pop("awaiting_broadcast", None)
        if text in ("отмена", "cancel"):
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        chat_ids = []
        if target == "all":
            chat_ids = [u["telegram_id"] for u in list_all_users()]
        elif target == "paid":
            chat_ids = list_paid_users()
        elif target == "refs":
            chat_ids = [s["telegram_id"] for s in get_referral_stats()]
        msg_text = update.message.text
        bot_to_use = _client_bot or context.bot
        sent, failed = 0, 0
        for cid in chat_ids:
            try:
                await bot_to_use.send_message(chat_id=cid, text=msg_text)
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(f"📢 Рассылка: отправлено {sent}, ошибок {failed}.", reply_markup=_main_menu_keyboard(True))
        return

    if context.user_data.get("awaiting_set_partner") and _is_owner(update.effective_user.id):
        if text in ("отмена", "cancel"):
            context.user_data.pop("awaiting_set_partner", None)
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        txt = update.message.text.strip().lstrip("@")
        user = get_user_by_username(txt) if not txt.isdigit() else get_user(int(txt))
        if user:
            set_partner(user["telegram_id"], True)
            context.user_data.pop("awaiting_set_partner", None)
            await update.message.reply_text(f"✅ {user.get('username') or user['telegram_id']} назначен партнёром (20%).", reply_markup=_main_menu_keyboard(True))
        else:
            await update.message.reply_text("⚠️ Пользователь не найден.")
        return

    if context.user_data.get("awaiting_set_discount") and _is_owner(update.effective_user.id):
        step = context.user_data["awaiting_set_discount"]
        if text in ("отмена", "cancel"):
            context.user_data.pop("awaiting_set_discount", None)
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        if step == "user":
            txt = update.message.text.strip().lstrip("@")
            user = get_user_by_username(txt) if not txt.isdigit() else get_user(int(txt))
            if user:
                context.user_data["awaiting_set_discount"] = {"user_id": user["telegram_id"]}
                await update.message.reply_text("Укажите процент скидки (например 15):")
            else:
                await update.message.reply_text("⚠️ Пользователь не найден.")
        elif isinstance(step, dict):
            try:
                pct = float(update.message.text.strip())
                if 0 <= pct <= 100:
                    set_custom_discount(step["user_id"], pct)
                    context.user_data.pop("awaiting_set_discount", None)
                    await update.message.reply_text(f"✅ Скидка {pct}% установлена.", reply_markup=_main_menu_keyboard(True))
                else:
                    await update.message.reply_text("⚠️ Процент от 0 до 100.")
            except ValueError:
                await update.message.reply_text("⚠️ Введите число.")
        return

    if context.user_data.get("awaiting_payment") and _is_admin(update.effective_user.id):
        payload = context.user_data["awaiting_payment"]
        if text in ("отмена", "cancel"):
            context.user_data.pop("awaiting_payment", None)
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
            return
        if payload == "amount":
            try:
                parts = update.message.text.strip().split()
                amount, days = float(parts[0]), int(parts[1])
                if amount > 0 and days in (30, 60, 90):
                    context.user_data["awaiting_payment"] = {"amount": amount, "days": days}
                    await update.message.reply_text("Укажите @username или ID пользователя:")
                else:
                    await update.message.reply_text("⚠️ Дни: 30, 60 или 90. Пример: 35 30")
            except (ValueError, TypeError, IndexError):
                await update.message.reply_text("⚠️ Формат: сумма дни (например 35 30)")
        elif isinstance(payload, dict):
            txt = update.message.text.strip().lstrip("@")
            user = get_user_by_username(txt) if not txt.isdigit() else get_user(int(txt))
            if user:
                add_payment(user["telegram_id"], payload["amount"], payload["days"])
                context.user_data.pop("awaiting_payment", None)
                await update.message.reply_text(f"✅ Платёж ${payload['amount']} за {payload['days']}д записан.", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
            else:
                await update.message.reply_text("⚠️ Пользователь не найден. Напишите @username или ID.")
        return


def _client_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Личный кабинет", callback_data="client_cabinet")],
        [InlineKeyboardButton("🛒 Купить подписку", callback_data="client_buy"), InlineKeyboardButton("🔑 Мой код", callback_data="client_mycode")],
        [InlineKeyboardButton("📥 Получить софт", callback_data="client_software")],
    ])


def _client_menu_button():
    """Кнопка «Вернуться в меню» для всех экранов клиента."""
    return [InlineKeyboardButton("◀️ В меню", callback_data="client_back")]


async def client_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except (BadRequest, TimedOut, NetworkError):
        return
    user_id = update.effective_user.id
    username = update.effective_user.username or ""
    if query.data == "client_cabinet":
        refs = list_referrals(user_id)
        payouts = get_user_payouts(user_id)
        pending = get_user_total_pending(user_id)
        bot_username = context.bot.username or "NeuralVoiceLabBot"
        ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        u = get_user(user_id)
        role = "🤝 Партнёр (20%)" if (u and u.get("is_partner")) else "👤 Клиент (10%)"
        text = (
            "👤 *Личный кабинет*\n\n"
            "━━━━━━━━━━━━━━━━\n"
            f"📌 {role}\n"
            f"👥 Рефералов: *{len(refs)}*\n"
            f"💰 К выплате: *${pending}*\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "Нажмите кнопку ниже, чтобы получить вашу реферальную ссылку."
        )
        kb = [
            [InlineKeyboardButton("🤝 Пригласить реферала", callback_data="client_invite")],
            [InlineKeyboardButton("📋 Мои выплаты", callback_data="client_payouts")],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_invite":
        bot_username = context.bot.username or "NeuralVoiceLabBot"
        ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        text = (
            "🤝 *Пригласить реферала*\n\n"
            "Поделитесь ссылкой — за каждого приглашённого вы получите процент с его покупок.\n\n"
            f"🔗 Ваша ссылка:\n`{ref_link}`\n\n"
            "Нажмите на ссылку и скопируйте, чтобы отправить друзьям."
        )
        kb = [
            [InlineKeyboardButton("◀️ В кабинет", callback_data="client_cabinet")],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_payouts":
        payouts = get_user_payouts(user_id)
        if not payouts:
            text = "📋 *Мои выплаты*\n\nИстория пуста."
        else:
            lines = [f"${p['amount_usd']} ({p['percent']}%) — {p['status']}" for p in payouts[:15]]
            text = "📋 *Мои выплаты*\n\n" + "\n".join(lines)
        kb = [
            [InlineKeyboardButton("◀️ Кабинет", callback_data="client_cabinet")],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_back":
        welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
        await query.edit_message_text(welcome, parse_mode="Markdown", reply_markup=_client_keyboard())
        return
    if query.data == "client_buy":
        price_30 = get_setting("price_30", "15")
        price_60 = get_setting("price_60", "25")
        price_90 = get_setting("price_90", "35")
        await query.edit_message_text(
            f"🛒 *Подписка*\n\n30 дней — ${price_30}\n60 дней — ${price_60}\n90 дней — ${price_90}\n\nНапишите «Оплатил» — администратор вышлет код после подтверждения.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([_client_menu_button()])
        )
        return
    if query.data == "client_software":
        url = get_setting("software_url", "https://drive.google.com/")
        await query.edit_message_text(f"📥 *Скачать VoiceLab*\n\n{url}\n\nРаспакуйте и запустите. Тест: 10 000 символов.", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([_client_menu_button()]))
        return
    if query.data == "client_mycode":
        rows = list_codes_and_activations()
        un = (username or "").lower().lstrip("@")
        my = [r for r in rows if not r.get("revoked") and (
            r.get("user_telegram_id") == user_id or
            (r.get("assigned_username") or "").lower() == un
        )]
        if not my:
            await query.edit_message_text(
                "У вас нет кода. Купите подписку и получите код от администратора.",
                reply_markup=InlineKeyboardMarkup([_client_menu_button()])
            )
        else:
            r = my[0]
            await query.edit_message_text(
                f"🔑 *Ваш код*\n\n`{r['code']}`\n\nДо: {r.get('expires_at') or 'бессрочно'}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([_client_menu_button()])
            )


async def client_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or ""
    referred_by = None
    if context.args and context.args[0].startswith("ref_"):
        try:
            referred_by = int(context.args[0].replace("ref_", ""))
            if referred_by == user_id:
                referred_by = None
        except ValueError:
            pass
    ensure_user(user_id, username, referred_by)
    welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
    await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=_client_keyboard())


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
