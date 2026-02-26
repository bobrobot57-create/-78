# -*- coding: utf-8 -*-
"""Обработчики ботов — общая логика для админ и клиент."""
import os
import asyncio

# Клиентский бот для рассылок и отправки кодов после оплаты (устанавливается из main.py)
_client_bot = None

def set_client_bot(bot):
    global _client_bot
    _client_bot = bot

def get_client_bot():
    return _client_bot
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TimedOut, NetworkError

try:
    from psycopg2.pool import PoolError
    from psycopg2 import OperationalError
except ImportError:
    PoolError = type("PoolError", (Exception,), {})
    OperationalError = type("OperationalError", (Exception,), {})
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, CallbackQueryHandler,
    filters,
)
from queue_pending import add_pending
from db import (
    create_code, create_codes_batch, revoke_code, list_codes_and_activations,
    get_owner_id, get_all_admin_ids, add_admin, remove_admin, list_admins, is_appointed_admin,
    set_code_assigned, delete_code, delete_all_codes, get_free_codes,
    set_pending_code_assign, get_pending_code_assign, clear_pending_code_assign,
    get_user_subscription_info, get_client_full_info,
    ensure_user, get_user, get_user_by_username, set_partner, set_custom_discount,
    set_gift, set_blocked,
    ensure_pending_user, get_pending_user, set_pending_blocked, set_pending_partner, set_pending_gift, set_pending_discount, merge_pending_to_user,
    list_referrals, add_payment, get_referral_stats, get_user_payouts, get_user_total_pending,
    list_all_users, list_paid_users, list_assigned_usernames_not_in_users, list_clients_with_extended,
    get_setting, get_setting_cached, set_setting, list_recent_payments,
)


def _fmt_date(val):
    """Форматирование даты для отображения (str или datetime от PostgreSQL)."""
    if not val:
        return "—"
    return str(val)[:10]


def _escape_md(s: str) -> str:
    """Экранирование для Markdown (underscore и др. ломают разбор)."""
    if not s:
        return s
    for c in "_*`[":
        s = str(s).replace(c, "\\" + c)
    return s


def _is_owner(user_id: int) -> bool:
    """Полные права: владелец (первый в ADMIN_USER_IDS) или любой админ из admins."""
    if get_owner_id() is not None and user_id == get_owner_id():
        return True
    return user_id in get_all_admin_ids() or is_appointed_admin(user_id)


def _is_admin(user_id: int) -> bool:
    return user_id in get_all_admin_ids() or is_appointed_admin(user_id)


async def _retry_db(func, *args, max_attempts=4, delay=3, **kwargs):
    """Повтор при PoolError/OperationalError — запрос в очереди, не падаем."""
    last_err = None
    for attempt in range(max_attempts):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except (PoolError, OperationalError) as e:
            last_err = e
            if attempt < max_attempts - 1:
                await asyncio.sleep(delay)
    raise last_err


def _main_menu_keyboard(is_owner: bool):
    kb = [
        [InlineKeyboardButton("🎁 Выдать код клиенту", callback_data="give_code_menu")],
        [InlineKeyboardButton("💰 Создать код", callback_data="create_code_menu")],
        [InlineKeyboardButton("📋 Список кодов", callback_data="list_codes")],
        [InlineKeyboardButton("👥 Список клиентов", callback_data="list_clients")],
        [InlineKeyboardButton("📊 Рефералы", callback_data="ref_stats")],
        [InlineKeyboardButton("📜 Логи платежей", callback_data="payments_log")],
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


CODES_LEGEND = "код | тип | @user | ст. | срок\n━━━━━━━━━━━━━━━━\n\n"


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
        if not exp_raw or r["is_developer"]:
            days_str = "∞"
        else:
            from db import _to_datetime
            exp = _to_datetime(exp_raw)
            days_str = f"{max(0, (exp - now).days)}д" if exp else "?"
        rev = " ❌" if r.get("revoked") else ""
        lines.append(f"`{r['code']}` {dev} {acc} {status} {days_str}{rev}")
        # Кнопка привязки только для свободных кодов — иначе перезапишем предыдущего клиента
        assign_btn = [InlineKeyboardButton("🔗", callback_data=f"a_{r['code']}")] if not r.get("assigned_username") else []
        kb.append(assign_btn + [InlineKeyboardButton("🗑", callback_data=f"d_{r['code']}")])
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
        await query.edit_message_text(
            f"🎛 *Панель VoiceLab*\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 Роль: {role}\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Выберите действие:",
            parse_mode="Markdown",
            reply_markup=_main_menu_keyboard(is_owner)
        )
        return
    if data == "create_code_menu":
        await query.edit_message_text("💰 *Создать код*\n\nВыберите тип:", parse_mode="Markdown", reply_markup=_create_code_keyboard())
        return
    if data == "give_code_menu":
        context.user_data.pop("awaiting_give_code_client", None)
        context.user_data.pop("awaiting_give_code_type", None)
        clear_pending_code_assign(user_id)
        free = get_free_codes(15)
        kb = []
        for c in free[:10]:
            dev = "♾" if c["is_developer"] else f"{c['days']}д"
            kb.append([InlineKeyboardButton(f"📌 {c['code'][:8]}... ({dev})", callback_data=f"gc_{c['code']}")])
        kb.append([InlineKeyboardButton("➕ Создать новый код", callback_data="give_code_new")])
        kb.append([InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
        text = "🎁 *Выдать код клиенту*\n\nВыберите свободный код или создайте новый:"
        if not free:
            text = "🎁 *Выдать код клиенту*\n\nНет свободных кодов. Создайте новый:"
            kb = [[InlineKeyboardButton("➕ Создать новый код", callback_data="give_code_new")], [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data.startswith("gc_") and len(data) > 3:
        code_val = data[3:]
        context.user_data["awaiting_give_code_client"] = code_val
        set_pending_code_assign(user_id, code_val)
        await query.edit_message_text(
            f"🔗 *Привязать код* `{code_val}`\n\nОтправьте @username или ссылку t.me/username клиента:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="give_code_menu")]])
        )
        return
    if data == "give_code_new":
        context.user_data["awaiting_give_code_type"] = True
        kb = [
            [InlineKeyboardButton("30 дней", callback_data="code_30"), InlineKeyboardButton("60 дней", callback_data="code_60"), InlineKeyboardButton("90 дней", callback_data="code_90")],
            [InlineKeyboardButton("♾ Вечный", callback_data="code_dev_1")],
            [InlineKeyboardButton("◀️ Назад", callback_data="give_code_menu")],
        ]
        await query.edit_message_text(
            "➕ *Создать и выдать код*\n\nВыберите тип:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return
    if data == "code_30":
        code = create_code(days=30)
        if context.user_data.pop("awaiting_give_code_type", None):
            context.user_data["awaiting_give_code_client"] = code
            set_pending_code_assign(user_id, code)
            await query.edit_message_text(
                f"✅ *Код создан* `{code}`\n\nОтправьте @username или ссылку t.me/username клиента:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="give_code_menu")]])
            )
        else:
            await query.edit_message_text(f"✅ *Код на 30 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "code_60":
        code = create_code(days=60)
        if context.user_data.pop("awaiting_give_code_type", None):
            context.user_data["awaiting_give_code_client"] = code
            set_pending_code_assign(user_id, code)
            await query.edit_message_text(
                f"✅ *Код создан* `{code}`\n\nОтправьте @username или ссылку t.me/username клиента:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="give_code_menu")]])
            )
        else:
            await query.edit_message_text(f"✅ *Код на 60 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "code_90":
        code = create_code(days=90)
        if context.user_data.pop("awaiting_give_code_type", None):
            context.user_data["awaiting_give_code_client"] = code
            set_pending_code_assign(user_id, code)
            await query.edit_message_text(
                f"✅ *Код создан* `{code}`\n\nОтправьте @username или ссылку t.me/username клиента:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="give_code_menu")]])
            )
        else:
            await query.edit_message_text(f"✅ *Код на 90 дней*\n\n`{code}`", parse_mode="Markdown", reply_markup=_back_to_menu_keyboard(is_owner))
        return
    if data == "code_dev_1":
        code = create_code(days=0, is_developer=True)
        if context.user_data.pop("awaiting_give_code_type", None):
            context.user_data["awaiting_give_code_client"] = code
            set_pending_code_assign(user_id, code)
            await query.edit_message_text(
                f"✅ *Код создан* `{code}`\n\nОтправьте @username или ссылку t.me/username клиента:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="give_code_menu")]])
            )
        else:
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
    if data == "payments_log":
        payments = list_recent_payments(25)
        if not payments:
            text = "📜 *Логи платежей*\n\n━━━━━━━━━━━━━━━━\n\nПока нет записей."
        else:
            lines = []
            for p in payments:
                sys_icon = "💳" if p["system"] == "freekassa" else ("₿" if p["system"] == "cryptomus" else "✏️")
                created = (p["created"] or "")[:16] if p.get("created") else ""
                lines.append(f"• {sys_icon} `{p['user_id']}` ${p['amount']} {p['days']}д · {p['system']} · {created}")
            text = "📜 *Логи платежей* (последние 25)\n\n━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data="payments_log")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]))
        return
    if data == "list_clients" or (data.startswith("list_clients") and ":" in data):
        parts = data.split(":")
        page = int(parts[1]) if len(parts) > 1 else 0
        sort_by = context.user_data.get("client_sort", "date")
        if len(parts) > 2 and parts[2] in ("date", "name", "status"):
            sort_by = parts[2]
            context.user_data["client_sort"] = sort_by
        search = context.user_data.get("client_search") or ""
        try:
            users = list_clients_with_extended(sort_by)
            if search:
                un = search.lower().lstrip("@")
                users = [u for u in users if un in (u.get("username") or "").lower() or str(u["telegram_id"]) == search]
            paid = set(list_paid_users())
            total = len(users)
            clients = sum(1 for u in users if not u.get("is_partner") and not u.get("is_gift"))
            partners = sum(1 for u in users if u.get("is_partner"))
            gifts = sum(1 for u in users if u.get("is_gift"))
            summary = f"👤 {clients} | 🤝 {partners} | 🎁 {gifts} | 💰 {len(paid)} оплатили"
            PAGE_SIZE = 10
            total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
            page = max(0, min(page, total_pages - 1))
            start = page * PAGE_SIZE
            page_users = users[start:start + PAGE_SIZE]
            lines, kb = [], []
            for u in page_users:
                un = f"@{u['username']}" if u.get("username") else f"ID:{u['telegram_id']}"
                un_safe = _escape_md(un)
                if u.get("is_blocked"): role = "🚫"
                elif u.get("is_partner"): role = "🤝"
                elif u.get("is_gift"): role = "🎁"
                else: role = "👤"
                pay_mark = "💰" if u["telegram_id"] in paid else "—"
                lines.append(f"{role} {un_safe} {pay_mark}")
                cid = u["telegram_id"] if u["telegram_id"] else f"u_{u.get('username','')}"
                kb.append([InlineKeyboardButton(f"📋 {un}", callback_data=f"client_{cid}")])
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("◀️", callback_data=f"list_clients:{page-1}:{sort_by}"))
            nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton("▶️", callback_data=f"list_clients:{page+1}:{sort_by}"))
            kb.append(nav)
            sort_btn = InlineKeyboardButton("📊 Сортировка", callback_data="client_sort_menu")
            footer = [InlineKeyboardButton("🔍 Поиск", callback_data="client_search"), InlineKeyboardButton("🔄", callback_data="list_clients"), sort_btn]
            if search:
                footer.insert(1, InlineKeyboardButton("✖", callback_data="client_search_clear"))
            footer.append(InlineKeyboardButton("◀️ Меню", callback_data="main_menu"))
            kb.append(footer)
            header = f"Поиск: @{search}\n\n" if search else ""
            text = f"👥 *Список клиентов* ({total})\n\n{summary}\n━━━━━━━━━━━━━━━━\n\n{header}" + "\n".join(lines)
            try:
                await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
            except BadRequest:
                await query.edit_message_text(text[:4000], reply_markup=InlineKeyboardMarkup(kb))
        except Exception as e:
            err_msg = "Сервер перегружен" if "перегружен" in str(e) or "pool" in str(e).lower() else "Ошибка загрузки"
            await query.edit_message_text(
                f"⚠️ {err_msg}. Подождите минуту и нажмите «Повторить».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Повторить", callback_data="list_clients"), InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]])
            )
        return
    if data == "client_sort_menu":
        sort = context.user_data.get("client_sort", "date")
        kb = [
            [InlineKeyboardButton("📅 По дате" + (" ✓" if sort == "date" else ""), callback_data="list_clients:0:date")],
            [InlineKeyboardButton("🔤 По имени" + (" ✓" if sort == "name" else ""), callback_data="list_clients:0:name")],
            [InlineKeyboardButton("📌 По статусу" + (" ✓" if sort == "status" else ""), callback_data="list_clients:0:status")],
            [InlineKeyboardButton("◀️ Назад", callback_data="list_clients")],
        ]
        await query.edit_message_text("📊 Сортировка списка:", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "client_search":
        context.user_data["awaiting_client_search"] = True
        await query.edit_message_text("🔍 Отправьте @username или ID для поиска:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="list_clients")]]))
        return
    if data == "client_search_clear":
        context.user_data.pop("client_search", None)
        context.user_data.pop("awaiting_client_search", None)
        try:
            users = list_clients_with_extended(context.user_data.get("client_sort", "date"))
        except Exception:
            await query.edit_message_text(
                "⚠️ Ошибка загрузки. Подождите минуту.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]])
            )
            return
        paid = set(list_paid_users())
        total = len(users)
        clients = sum(1 for u in users if not u.get("is_partner") and not u.get("is_gift"))
        partners = sum(1 for u in users if u.get("is_partner"))
        gifts = sum(1 for u in users if u.get("is_gift"))
        summary = f"👤 {clients} | 🤝 {partners} | 🎁 {gifts} | 💰 {len(paid)} оплатили"
        PAGE_SIZE = 10
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page_users = users[:PAGE_SIZE]
        lines, kb = [], []
        for u in page_users:
            un = f"@{u['username']}" if u.get("username") else f"ID:{u['telegram_id']}"
            un_safe = _escape_md(un)
            if u.get("is_blocked"): role = "🚫"
            elif u.get("is_partner"): role = "🤝"
            elif u.get("is_gift"): role = "🎁"
            else: role = "👤"
            pay_mark = "💰" if u["telegram_id"] in paid else "—"
            lines.append(f"{role} {un_safe} {pay_mark}")
            cid = u["telegram_id"] if u["telegram_id"] else f"u_{u.get('username','')}"
            kb.append([InlineKeyboardButton(f"📋 {un}", callback_data=f"client_{cid}")])
        sort_by = context.user_data.get("client_sort", "date")
        nav = [InlineKeyboardButton("1/" + str(total_pages), callback_data="noop")]
        if total_pages > 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"list_clients:1:{sort_by}"))
        kb.append(nav)
        kb.append([InlineKeyboardButton("🔍 Поиск", callback_data="client_search"), InlineKeyboardButton("🔄", callback_data="list_clients"), InlineKeyboardButton("📊 Сорт.", callback_data="client_sort_menu"), InlineKeyboardButton("◀️ Меню", callback_data="main_menu")])
        text = f"👥 *Список клиентов* ({total})\n\n{summary}\n━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines)
        try:
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        except BadRequest:
            await query.edit_message_text(text[:4000], reply_markup=InlineKeyboardMarkup(kb))
        return
    # Сначала проверяем действия (partner/gift/block/pct), иначе client_partner_123_1 попадёт сюда и упадёт
    if data.startswith("client_partner_") and is_owner:
        rest = data.replace("client_partner_", "")
        if rest.startswith("u_"):
            parts = rest[2:].rsplit("_", 1)
            if len(parts) == 2:
                un, is_part = parts[0], int(parts[1])
                set_pending_partner(un, bool(is_part))
                info = get_client_full_info(0, un)
                if info:
                    un_display = f"@{info['username']}" if info.get("username") else un
                    role = "партнёром (20%)" if is_part else "клиентом (10%)"
                    await query.edit_message_text(f"✅ {un_display} назначен {role}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_u_{un}")]]))
        else:
            parts = rest.split("_")
            if len(parts) == 2:
                uid, is_part = int(parts[0]), int(parts[1])
                set_partner(uid, bool(is_part))
                info = get_client_full_info(uid)
                if info:
                    un = f"@{info['username']}" if info.get("username") else f"ID:{info['telegram_id']}"
                    role = "партнёром (20%)" if is_part else "клиентом (10%)"
                    await query.edit_message_text(f"✅ {un} назначен {role}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_{uid}")]]))
        return
    if data.startswith("client_gift_") and is_owner:
        rest = data.replace("client_gift_", "")
        if rest.startswith("u_"):
            parts = rest[2:].rsplit("_", 1)
            if len(parts) == 2:
                un, is_gift = parts[0], int(parts[1])
                set_pending_gift(un, bool(is_gift))
                info = get_client_full_info(0, un)
                if info:
                    un_display = f"@{info['username']}" if info.get("username") else un
                    role = "подарком (10%)" if is_gift else "клиентом"
                    await query.edit_message_text(f"✅ {un_display} назначен {role}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_u_{un}")]]))
        else:
            parts = rest.split("_")
            if len(parts) == 2:
                uid, is_gift = int(parts[0]), int(parts[1])
                set_gift(uid, bool(is_gift))
                info = get_client_full_info(uid)
                if info:
                    un = f"@{info['username']}" if info.get("username") else f"ID:{info['telegram_id']}"
                    role = "подарком (10%)" if is_gift else "клиентом"
                    await query.edit_message_text(f"✅ {un} назначен {role}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_{uid}")]]))
        return
    if data.startswith("client_block_") and is_owner:
        rest = data.replace("client_block_", "")
        if rest.startswith("u_"):
            un = rest[2:].rsplit("_", 1)[0] if "_" in rest[2:] else rest[2:]
            set_pending_blocked(un, True)
            info = get_client_full_info(0, un)
            if info:
                un_display = f"@{info['username']}" if info.get("username") else un
                await query.edit_message_text(f"✅ {un_display} заблокирован навсегда.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_u_{un}")]]))
        else:
            parts = rest.split("_")
            if len(parts) == 2:
                uid, is_block = int(parts[0]), int(parts[1])
                set_blocked(uid, bool(is_block))
                info = get_client_full_info(uid)
                if info:
                    un = f"@{info['username']}" if info.get("username") else f"ID:{info['telegram_id']}"
                    await query.edit_message_text(f"✅ {un} заблокирован навсегда.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К клиенту", callback_data=f"client_{uid}")]]))
        return
    if data.startswith("client_pct_") and is_owner:
        rest = data.replace("client_pct_", "")
        if rest.startswith("u_"):
            un = rest[2:]
            context.user_data["awaiting_client_pct"] = f"u_{un}"
            un_display = f"@{un}" if un else ""
            await query.edit_message_text(
                f"✏️ Укажите процент рефералки для {un_display} (0–100):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"client_u_{un}")]])
            )
        else:
            uid = int(rest)
            context.user_data["awaiting_client_pct"] = uid
            info = get_client_full_info(uid)
            un = f"@{info['username']}" if info and info.get("username") else f"ID:{uid}"
            await query.edit_message_text(
                f"✏️ Укажите процент рефералки для {un} (0–100):",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"client_{uid}")]])
            )
        return
    if data.startswith("client_") and data != "client_search" and data != "client_search_clear" and "client_sort" not in data:
        cid_raw = data.replace("client_", "")
        uid, un_param = None, None
        if cid_raw.startswith("u_"):
            un_param = cid_raw[2:]
            uid = 0
        else:
            try:
                uid = int(cid_raw)
            except ValueError:
                return
        try:
            info = await _retry_db(get_client_full_info, uid, un_param) if un_param else await _retry_db(get_client_full_info, uid)
        except (PoolError, OperationalError):
            add_pending(update, context.application.update_queue)
            await query.edit_message_text(
                "⏳ Обрабатываю...",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ К списку", callback_data="list_clients")]])
            )
            return
        if not info:
            await query.edit_message_text("❌ Клиент не найден.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="list_clients")]]))
            return
        un = f"@{info['username']}" if info.get("username") else f"ID:{info['telegram_id']}"
        if info.get("is_blocked"): role = "🚫 Заблокирован"
        elif info.get("is_gift"): role = "🎁 Подарок"
        elif info.get("is_partner"): role = "🤝 Партнёр"
        else: role = "👤 Клиент"
        pct = info.get("percent", 10)
        sub = info.get("subscription")
        sub_block = "—"
        if sub:
            if sub["status"] == "activated":
                days = info.get("days_left")
                sub_block = f"`{sub['code']}` · {'∞' if days == '∞' else f'{days} дн.'}"
            else:
                sub_block = f"`{sub['code']}` (ожидает активации)"
        first_seen = _fmt_date(info.get("first_seen"))
        text = (
            f"👤 *Клиент* {un}\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 Статус: {role}\n"
            f"📊 Реф. процент: *{pct}%*\n"
            f"🔑 Код: {sub_block}\n"
            f"👥 Привёл рефералов: {info.get('ref_count', 0)}\n"
            f"💰 К выплате: ${info.get('pending_usd', 0)}\n"
            f"📅 В системе с: {first_seen}\n"
            f"🔗 Пригласил: {info.get('referrer') or '—'}\n"
            f"━━━━━━━━━━━━━━━━"
        )
        kb = []
        if is_owner:
            if info.get("_assigned_only") and un_param:
                un_safe = un_param.replace(" ", "_")[:32]
                if not info.get("is_blocked"):
                    kb.append([InlineKeyboardButton("🚫 Заблокировать (навсегда)", callback_data=f"client_block_u_{un_safe}_1")])
                row = []
                if not info.get("is_partner"):
                    row.append(InlineKeyboardButton("🤝 Партнёр (20%)", callback_data=f"client_partner_u_{un_safe}_1"))
                if not info.get("is_gift"):
                    row.append(InlineKeyboardButton("🎁 Подарок (10%)", callback_data=f"client_gift_u_{un_safe}_1"))
                if info.get("is_partner") or info.get("is_gift"):
                    row.append(InlineKeyboardButton("👤 Клиент (10%)", callback_data=f"client_partner_u_{un_safe}_0"))
                if row:
                    kb.append(row)
                kb.append([InlineKeyboardButton("✏️ Изменить % рефералки", callback_data=f"client_pct_u_{un_safe}")])
            elif uid and not info.get("_assigned_only"):
                if not info.get("is_blocked"):
                    kb.append([InlineKeyboardButton("🚫 Заблокировать (навсегда)", callback_data=f"client_block_{uid}_1")])
                row = []
                if not info.get("is_partner"):
                    row.append(InlineKeyboardButton("🤝 Партнёр (20%)", callback_data=f"client_partner_{uid}_1"))
                if not info.get("is_gift"):
                    row.append(InlineKeyboardButton("🎁 Подарок (10%)", callback_data=f"client_gift_{uid}_1"))
                if info.get("is_partner") or info.get("is_gift"):
                    row.append(InlineKeyboardButton("👤 Клиент (10%)", callback_data=f"client_partner_{uid}_0"))
                if row:
                    kb.append(row)
                kb.append([InlineKeyboardButton("✏️ Изменить % рефералки", callback_data=f"client_pct_{uid}")])
        kb.append([InlineKeyboardButton("◀️ К списку", callback_data="list_clients")])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
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
            role = "🤝 Партнёр" if s.get("is_partner") else ("🎁 Подарок" if s.get("is_gift") else "👤 Клиент")
            pct = s["percent"]
            un = f"@{s['username']}" if s.get("username") else f"ID:{s['telegram_id']}"
            lines.append(f"• {role} {un}\n  Рефералов: {s['ref_count']} | Ставка: {pct}% | К выплате: ${s['pending_usd']}")
        kb = [
            [InlineKeyboardButton("➕ Записать платёж", callback_data="record_payment")],
            [InlineKeyboardButton("🤝 Назначить партнёра", callback_data="set_partner")],
            [InlineKeyboardButton("✏️ Скидка для реферала", callback_data="set_discount")],
            [InlineKeyboardButton("🔄 Обновить", callback_data="ref_stats")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            "📊 *Рефералы*\n\n━━━━━━━━━━━━━━━━\n\n" + "\n\n".join(lines) + "\n\n━━━━━━━━━━━━━━━━",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return
    if data == "record_payment" and is_owner:
        context.user_data["awaiting_payment"] = "amount"
        await query.edit_message_text("➕ *Записать платёж*\n\nОтправьте: сумма долларов, дни\nНапример: `35 30`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="ref_stats")]]))
        return
    if data == "settings_menu" and is_owner:
        welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
        price_30 = get_setting("price_30", "35")
        price_60 = get_setting("price_60", "70")
        price_90 = get_setting("price_90", "100")
        software_url = get_setting("software_url", "https://drive.google.com/")
        fk_ok = "✅" if get_setting("fk_merchant_id", "") else "❌"
        cm_ok = "✅" if get_setting("cryptomus_merchant", "") else "❌"
        cards_on = get_setting("payments_cards_enabled", "1") == "1"
        crypto_on = get_setting("payments_crypto_enabled", "1") == "1"
        manual_contact = get_setting("manual_payment_contact", "@Drykey")
        text = (
            f"⚙️ *Настройки*\n\n"
            f"Приветствие: _{welcome[:50]}..._\n\n"
            f"Цены (USD): 30д={price_30} | 60д={price_60} | 90д={price_90}\n"
            f"Софт: {software_url[:40]}...\n\n"
            f"💳 Карты (FreeKassa): {'✅ Вкл' if cards_on else '❌ Выкл'} {fk_ok}\n"
            f"₿ Крипто (Cryptomus): {'✅ Вкл' if crypto_on else '❌ Выкл'} {cm_ok}\n"
            f"📩 Контакт: {manual_contact}\n\n"
            f"_Если оба выкл — клиент видит только контакт партнёра._"
        )
        kb = [
            [InlineKeyboardButton("💳 Карты вкл/выкл", callback_data="toggle_cards"), InlineKeyboardButton("₿ Крипто вкл/выкл", callback_data="toggle_crypto")],
            [InlineKeyboardButton("📩 Контакт при выкл", callback_data="set_manual_contact")],
            [InlineKeyboardButton("✏️ Приветствие", callback_data="set_welcome")],
            [InlineKeyboardButton("💵 Цены", callback_data="set_prices")],
            [InlineKeyboardButton("📥 Ссылка на софт", callback_data="set_software_url")],
            [InlineKeyboardButton("💳 FreeKassa", callback_data="set_freekassa"), InlineKeyboardButton("₿ Cryptomus", callback_data="set_cryptomus")],
            [InlineKeyboardButton("◀️ Меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if data == "toggle_cards" and is_owner:
        cur = "1" if get_setting("payments_cards_enabled", "1") != "1" else "0"
        set_setting("payments_cards_enabled", cur)
        status = "включена" if cur == "1" else "выключена"
        await query.edit_message_text(f"✅ Оплата картой {status}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Настройки", callback_data="settings_menu")]]))
        return
    if data == "toggle_crypto" and is_owner:
        cur = "1" if get_setting("payments_crypto_enabled", "1") != "1" else "0"
        set_setting("payments_crypto_enabled", cur)
        status = "включена" if cur == "1" else "выключена"
        await query.edit_message_text(f"✅ Оплата криптой {status}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Настройки", callback_data="settings_menu")]]))
        return
    if data == "set_manual_contact" and is_owner:
        context.user_data["awaiting_setting"] = "manual_payment_contact"
        await query.edit_message_text(
            "📩 Отправьте @username или контакт для клиентов (когда оплата выключена):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]])
        )
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
        await query.edit_message_text("💵 Отправьте цены через пробел: 30д 60д 90д\nНапример: 35 70 100", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]]))
        return
    if data == "set_software_url" and is_owner:
        context.user_data["awaiting_setting"] = "software_url"
        await query.edit_message_text("📥 Отправьте ссылку (Google Drive или любую другую):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]]))
        return
    if data == "set_freekassa" and is_owner:
        context.user_data["awaiting_setting"] = "freekassa"
        await query.edit_message_text(
            "💳 *FreeKassa*\n\nОтправьте через пробел:\n`merchant_id secret1 secret2`\n\nПример: 12345 abcdef secret2word",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]])
        )
        return
    if data == "set_cryptomus" and is_owner:
        context.user_data["awaiting_setting"] = "cryptomus"
        await query.edit_message_text(
            "₿ *Cryptomus*\n\nОтправьте через пробел:\n`merchant_uuid api_key`\n\nUUID и ключ из личного кабинета Cryptomus.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="settings_menu")]])
        )
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

    if context.user_data.get("awaiting_client_search"):
        context.user_data.pop("awaiting_client_search", None)
        if text in ("отмена", "cancel"):
            context.user_data.pop("client_search", None)
            await update.message.reply_text("Отменено.")
            return
        context.user_data["client_search"] = text.strip().lstrip("@")
        await update.message.reply_text(f"Поиск: {context.user_data['client_search']}. Нажмите «Список клиентов» в меню.")
        return

    if context.user_data.get("awaiting_client_pct") and _is_owner(update.effective_user.id):
        target = context.user_data.pop("awaiting_client_pct", None)
        if text in ("отмена", "cancel"):
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        if target is not None:
            try:
                pct = float(update.message.text.strip().replace(",", "."))
                if 0 <= pct <= 100:
                    if isinstance(target, str) and target.startswith("u_"):
                        un = target[2:]
                        set_pending_discount(un, pct)
                        un_display = f"@{un}"
                        await update.message.reply_text(f"✅ Реферальный процент для {un_display}: {pct}%", reply_markup=_main_menu_keyboard(True))
                    else:
                        set_custom_discount(target, pct)
                        info = get_client_full_info(target)
                        un = f"@{info['username']}" if info and info.get("username") else f"ID:{target}"
                        await update.message.reply_text(f"✅ Реферальный процент для {un}: {pct}%", reply_markup=_main_menu_keyboard(True))
                else:
                    await update.message.reply_text("⚠️ Процент от 0 до 100.")
                    context.user_data["awaiting_client_pct"] = target
            except ValueError:
                await update.message.reply_text("⚠️ Введите число.")
                context.user_data["awaiting_client_pct"] = target
        return

    # Сначала — явные «ожидаю ввод» (настройки, рассылка и т.д.), иначе «1 10 100» уйдёт в выдачу кода
    if context.user_data.get("awaiting_setting") and _is_owner(update.effective_user.id):
        key = context.user_data.pop("awaiting_setting", None)
        if text in ("отмена", "cancel"):
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(True))
            return
        try:
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
            elif key == "freekassa":
                parts = update.message.text.strip().split()
                if len(parts) >= 3:
                    set_setting("fk_merchant_id", parts[0])
                    set_setting("fk_secret_1", parts[1])
                    set_setting("fk_secret_2", parts[2])
                    await update.message.reply_text("✅ FreeKassa настроен.", reply_markup=_main_menu_keyboard(True))
                else:
                    await update.message.reply_text("⚠️ Нужно 3 значения: merchant_id secret1 secret2")
                    context.user_data["awaiting_setting"] = "freekassa"
            elif key == "cryptomus":
                parts = update.message.text.strip().split()
                if len(parts) >= 2:
                    set_setting("cryptomus_merchant", parts[0])
                    set_setting("cryptomus_api_key", parts[1])
                    await update.message.reply_text("✅ Cryptomus настроен.", reply_markup=_main_menu_keyboard(True))
                else:
                    await update.message.reply_text("⚠️ Нужно 2 значения: merchant_uuid api_key")
                    context.user_data["awaiting_setting"] = "cryptomus"
            elif key == "manual_payment_contact":
                set_setting("manual_payment_contact", update.message.text.strip() or "@Drykey")
                await update.message.reply_text("✅ Контакт обновлён.", reply_markup=_main_menu_keyboard(True))
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Ошибка сохранения настройки: %s", e)
            context.user_data["awaiting_setting"] = key
            await update.message.reply_text(f"⚠️ Ошибка: {e}. Попробуйте снова или нажмите «Отмена».")
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

    code_val = context.user_data.pop("awaiting_give_code_client", None) or get_pending_code_assign(update.effective_user.id)
    if code_val:
        if text in ("отмена", "cancel"):
            clear_pending_code_assign(update.effective_user.id)
            await update.message.reply_text("Отменено.", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
            return
        raw = update.message.text.strip()
        un = raw.lstrip("@")
        if "t.me/" in un.lower():
            un = un.split("t.me/")[-1].split("/")[0].split("?")[0]
        else:
            un = un.lstrip("@")
        if not un:
            await update.message.reply_text("⚠️ Укажите @username или ссылку t.me/username")
            context.user_data["awaiting_give_code_client"] = code_val
            set_pending_code_assign(update.effective_user.id, code_val)
            return
        if set_code_assigned(code_val, un):
            clear_pending_code_assign(update.effective_user.id)
            user = get_user_by_username(un)
            sent = False
            if user:
                client_bot = get_client_bot()
                if client_bot:
                    try:
                        await client_bot.send_message(
                            user["telegram_id"],
                            f"🎁 *Вам выдан код подписки VoiceLab*\n\n`{code_val}`\n\nАктивируйте в программе.",
                            parse_mode="Markdown"
                        )
                        sent = True
                    except Exception:
                        pass
            msg = f"✅ Код привязан к @{un}."
            if sent:
                msg += " Код отправлен клиенту в бота."
            else:
                msg += " ЛК и код появятся при первом заходе клиента в бота."
            await update.message.reply_text(msg, reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
        else:
            await update.message.reply_text("❌ Ошибка привязки. Проверьте код.", reply_markup=_main_menu_keyboard(_is_owner(update.effective_user.id)))
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
    u = get_user(user_id)
    if u and u.get("is_blocked"):
        await query.edit_message_text("⛔ Доступ ограничен. Обратитесь к администратору.", reply_markup=InlineKeyboardMarkup([_client_menu_button()]))
        return
    if query.data == "client_cabinet":
        refs = list_referrals(user_id)
        payouts = get_user_payouts(user_id)
        pending = get_user_total_pending(user_id)
        bot_username = context.bot.username or "NeuralVoiceLabBot"
        ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        u = get_user(user_id)
        role = "🤝 Партнёр (20%)" if (u and u.get("is_partner")) else ("🎁 Подарок (10%)" if (u and u.get("is_gift")) else "👤 Клиент (10%)")
        sub = get_user_subscription_info(user_id, username)
        sub_block = ""
        if sub:
            if sub["status"] == "activated":
                if sub["is_developer"]:
                    sub_block = "📦 *Подписка:* ♾ Бессрочная\n"
                elif sub["expires_at"]:
                    from datetime import datetime
                    from db import _to_datetime
                    exp = _to_datetime(sub["expires_at"])
                    days_left = max(0, (exp - datetime.utcnow()).days) if exp else 0
                    sub_block = f"📦 *Подписка:* {days_left} дн. осталось\n"
                else:
                    sub_block = "📦 *Подписка:* активна\n"
            else:
                sub_block = f"📦 *Код выдан:* `{sub['code']}` — активируйте в софте\n"
        text = (
            "👤 *Личный кабинет*\n\n"
            "━━━━━━━━━━━━━━━━\n"
            f"📌 {role}\n"
            + (sub_block if sub_block else "")
            + f"👥 Рефералов: *{len(refs)}*\n"
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
    if query.data in ("client_back", "main_menu"):
        # main_menu — от обработчика ошибок, ведёт в главное меню
        welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
        await query.edit_message_text(welcome, parse_mode="Markdown", reply_markup=_client_keyboard())
        return
    if query.data == "client_buy":
        cards_enabled = get_setting_cached("payments_cards_enabled", "1") == "1"
        crypto_enabled = get_setting_cached("payments_crypto_enabled", "1") == "1"
        manual_contact = get_setting_cached("manual_payment_contact", "@Drykey")
        # Из кэша — цены обновляются сразу после сохранения в админке
        price_30 = float(get_setting_cached("price_30", "35"))
        price_60 = float(get_setting_cached("price_60", "70"))
        price_90 = float(get_setting_cached("price_90", "100"))
        from payment import generate_freekassa_link, create_cryptomus_invoice
        import os
        webhook_base = os.environ.get("WEBHOOK_BASE_URL", "").rstrip("/")
        fk_30 = generate_freekassa_link(user_id, price_30, 30)
        fk_60 = generate_freekassa_link(user_id, price_60, 60)
        fk_90 = generate_freekassa_link(user_id, price_90, 90)
        has_fk = bool(fk_30 and fk_60 and fk_90)
        cm_merchant = get_setting("cryptomus_merchant", "") or os.environ.get("CRYPTOMUS_MERCHANT", "")
        cm_key = get_setting("cryptomus_api_key", "") or os.environ.get("CRYPTOMUS_API_KEY", "")
        has_cm = bool(cm_merchant and cm_key)
        show_cards = has_fk and cards_enabled
        show_crypto = has_cm and crypto_enabled
        # Оба выкл или оба не настроены — только контакт партнёра
        if not show_cards and not show_crypto:
            text = (
                "🛒 *Магазин подписок VoiceLab*\n\n"
                "🎙 Профессиональная озвучка текста нейросетью\n\n"
                "━━━━━━━━━━━━━━━━\n"
                "📦 *30 дней* | *60 дней* | *90 дней*\n"
                "━━━━━━━━━━━━━━━━\n\n"
                "💳 Онлайн-оплата недоступна.\n\n"
                f"📩 По всем вопросам пишите: {manual_contact}"
            )
            kb = [_client_menu_button()]
            await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
            return
        text = (
            "🛒 *Магазин подписок VoiceLab*\n\n"
            "🎙 Профессиональная озвучка текста нейросетью\n\n"
            "━━━━━━━━━━━━━━━━\n"
            f"📦 *30 дней* — ${price_30}  _(выгодно попробовать)_\n"
            f"📦 *60 дней* — ${price_60}  _(оптимально)_\n"
            f"📦 *90 дней* — ${price_90}  _(макс. выгода)_\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "💡 Выберите способ оплаты:\n\n"
            "✅ Ключ придёт сюда автоматически после оплаты.\n\n"
            f"📩 По всем вопросам пишите: {manual_contact}"
        )
        kb = []
        if show_cards:
            kb.append([InlineKeyboardButton("💳 Оплата картой", callback_data="client_pay_cards")])
        if show_crypto:
            kb.append([InlineKeyboardButton("₿ Оплата криптой", callback_data="client_pay_crypto")])
        kb.append(_client_menu_button())
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_pay_cards":
        price_30 = float(get_setting_cached("price_30", "35"))
        price_60 = float(get_setting_cached("price_60", "70"))
        price_90 = float(get_setting_cached("price_90", "100"))
        from payment import generate_freekassa_link
        fk_30 = generate_freekassa_link(user_id, price_30, 30)
        fk_60 = generate_freekassa_link(user_id, price_60, 60)
        fk_90 = generate_freekassa_link(user_id, price_90, 90)
        if not (fk_30 and fk_60 and fk_90):
            await query.edit_message_text("⚠️ Оплата картой временно недоступна.", reply_markup=InlineKeyboardMarkup([_client_menu_button()]))
            return
        text = (
            "💳 *Оплата картой*\n\n"
            "Выберите срок подписки:\n\n"
            f"📦 30 дней — ${price_30}\n"
            f"📦 60 дней — ${price_60}\n"
            f"📦 90 дней — ${price_90}\n\n"
            "✅ Ключ придёт сюда после оплаты."
        )
        kb = [
            [InlineKeyboardButton("💳 30 дней", url=fk_30), InlineKeyboardButton("💳 60 дней", url=fk_60), InlineKeyboardButton("💳 90 дней", url=fk_90)],
            [InlineKeyboardButton("◀️ Назад", callback_data="client_buy")],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_pay_crypto":
        price_30 = float(get_setting_cached("price_30", "35"))
        price_60 = float(get_setting_cached("price_60", "70"))
        price_90 = float(get_setting_cached("price_90", "100"))
        text = (
            "₿ *Оплата криптовалютой*\n\n"
            "Выберите срок подписки:\n\n"
            f"📦 30 дней — ${price_30}\n"
            f"📦 60 дней — ${price_60}\n"
            f"📦 90 дней — ${price_90}\n\n"
            "✅ Ключ придёт сюда после оплаты."
        )
        kb = [
            [InlineKeyboardButton("₿ 30 дней", callback_data="pay_cm_30"), InlineKeyboardButton("₿ 60 дней", callback_data="pay_cm_60"), InlineKeyboardButton("₿ 90 дней", callback_data="pay_cm_90")],
            [InlineKeyboardButton("◀️ Назад", callback_data="client_buy")],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data and query.data.startswith("pay_cm_"):
        plan_days = int(query.data.replace("pay_cm_", ""))
        if plan_days not in (30, 60, 90):
            return
        price_key = f"price_{plan_days}"
        amount = float(get_setting_cached(price_key, "35" if plan_days == 30 else "70" if plan_days == 60 else "100"))
        import os
        webhook_base = os.environ.get("WEBHOOK_BASE_URL", "").rstrip("/")
        if not webhook_base:
            await query.edit_message_text("⚠️ Сервер не настроен. Обратитесь к администратору.", reply_markup=InlineKeyboardMarkup([_client_menu_button()]))
            return
        import time
        order_id = f"cm_{user_id}_{plan_days}_{int(time.time())}"
        url_cb = f"{webhook_base}/payment/cryptomus"
        from payment import create_cryptomus_invoice
        inv = create_cryptomus_invoice(amount, order_id, user_id, plan_days, url_cb)
        if inv and inv.get("url"):
            await query.edit_message_text(
                f"₿ *Оплата {plan_days} дней (${amount})*\n\nПерейдите по ссылке для оплаты криптовалютой. Ключ придёт сюда после подтверждения.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Перейти к оплате", url=inv["url"])],
                    [InlineKeyboardButton("◀️ Назад", callback_data="client_pay_crypto")],
                    _client_menu_button(),
                ])
            )
        else:
            await query.edit_message_text("⚠️ Не удалось создать ссылку. Попробуйте позже или выберите оплату картой.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="client_pay_crypto")], _client_menu_button()]))
        return
    if query.data == "client_software":
        # Из кэша — без БД, меню показывается сразу
        url = get_setting_cached("software_url", "https://drive.google.com/drive/folders/18hdLnr_zPo7_Eao9thFQkp2H4nbgtLIa").strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        text = (
            "📥 *Как получить VoiceLab*\n\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ Откройте ссылку на Google Drive\n\n"
            "2️⃣ Скачайте архив\n\n"
            "3️⃣ Распакуйте на рабочий стол\n\n"
            "4️⃣ Запустите exe\n\n"
            "5️⃣ Используйте 10 000 бесплатных символов\n\n"
            "━━━━━━━━━━━━━━━━\n\n"
            "💎 *Если понравится — купите лицензию*\n\n"
            "6️⃣ В софте нажмите «Ввести код»\n\n"
            "7️⃣ Введите код и нажмите на серую плашку ввода\n\n"
            "8️⃣ Лицензия активируется полностью ✅"
        )
        kb = [
            [InlineKeyboardButton("🔗 Открыть Google Drive", url=url)],
            _client_menu_button(),
        ]
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return
    if query.data == "client_mycode":
        sub = get_user_subscription_info(user_id, username)
        if not sub:
            await query.edit_message_text(
                "У вас нет кода. Купите подписку и получите код от администратора.",
                reply_markup=InlineKeyboardMarkup([_client_menu_button()])
            )
        else:
            exp_str = "бессрочно" if sub["is_developer"] or not sub["expires_at"] else _fmt_date(sub["expires_at"])
            status_hint = "Активируйте в софте." if sub["status"] == "assigned" else f"До: {exp_str}"
            await query.edit_message_text(
                f"🔑 *Ваш код*\n\n`{sub['code']}`\n\n{status_hint}",
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
    merge_pending_to_user(user_id, username)
    u = get_user(user_id)
    if u and u.get("is_blocked"):
        await update.message.reply_text("⛔ Доступ ограничен. Обратитесь к администратору.")
        return
    welcome = get_setting("welcome_message", "🎙 *VoiceLab* — озвучка текста\n\nОплатите подписку и напишите «Оплатил».")
    await update.message.reply_text(welcome, parse_mode="Markdown", reply_markup=_client_keyboard())


async def client_mycode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or ""
    sub = get_user_subscription_info(user_id, username)
    if not sub:
        await update.message.reply_text("У вас нет кода.")
    else:
        exp_str = "бессрочно" if sub["is_developer"] or not sub["expires_at"] else sub["expires_at"][:10]
        status_hint = "Активируйте в софте." if sub["status"] == "assigned" else f"До: {exp_str}"
        await update.message.reply_text(f"Код: `{sub['code']}`\n\n{status_hint}", parse_mode="Markdown")


def _looks_like_activate(text: str) -> tuple[bool, str, str, str]:
    """Проверяет формат: КОД HWID [INST_ID]. Возвращает (ok, code, hwid, inst_id)."""
    parts = (text or "").strip().split()
    if len(parts) < 2:
        return False, "", "", ""
    code, hwid = parts[0].strip().upper(), parts[1].strip()
    inst_id = parts[2].strip() if len(parts) > 2 else ""
    if len(code) != 16 or not all(c in "0123456789ABCDEF" for c in code):
        return False, "", "", ""
    if len(hwid) != 32 or not all(c in "0123456789abcdef" for c in hwid.lower()):
        return False, "", "", ""
    if inst_id and (len(inst_id) != 32 or not all(c in "0123456789abcdef" for c in inst_id.lower())):
        return False, "", "", ""
    return True, code, hwid, inst_id


async def client_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    text_lower = text.lower()

    # VL_CHECK — запрос от exe через Telethon (проверка лицензии через ТГ)
    if text.startswith("VL_CHECK "):
        parts = text[8:].strip().split()
        if len(parts) >= 2:
            code, hwid = parts[0].strip().upper(), parts[1].strip()
            inst_id = parts[2].strip() if len(parts) > 2 else None
            from db import activate_code
            result = activate_code(code, hwid, inst_id)
            if result.get("ok"):
                exp = result.get("expires_at") or ""
                dev = "1" if result.get("is_developer") else "0"
                await update.message.reply_text(f"VL_OK:{exp}|{dev}")
            else:
                err = result.get("error", "unknown")
                await update.message.reply_text(f"VL_FAIL:{err}")
        return

    # Активация через ТГ: КОД HWID [INST_ID]
    ok, code, hwid, inst_id = _looks_like_activate(text)
    if ok:
        from token_utils import create_activation_token
        ok_token, result = create_activation_token(code, hwid, inst_id)
        if ok_token:
            await update.message.reply_text(
                f"✅ Токен активации (действует 15 мин):\n\n`{result}`\n\nСкопируй и вставь в окно VoiceLab.",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(f"❌ {result}")
        return
    if "оплатил" in text_lower or "купить" in text_lower:
        manual_contact = get_setting("manual_payment_contact", "@Drykey")
        await update.message.reply_text(f"По всем вопросам пишите: {manual_contact}\n\nОплатите подписку в меню «Купить подписку» — ключ придёт автоматически.")


async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    import logging
    _log = logging.getLogger(__name__)
    if isinstance(context.error, (TimedOut, NetworkError)):
        return
    # PoolError / OperationalError (connection pool) — показываем «Обрабатываю», не «Ошибка»
    err = context.error
    err_str = str(err).lower()
    is_db_overload = isinstance(err, PoolError) or (
        isinstance(err, OperationalError)
        and any(x in err_str for x in ("connection", "pool", "exhausted", "too many"))
    )
    if is_db_overload:
        _log.warning("DB overload: %s", err)
        try:
            target_queue = getattr(context.application, "update_queue", None)
            if target_queue:
                add_pending(update, target_queue)
            q = getattr(update, "callback_query", None)
            msg = getattr(update, "message", None)
            err_text = "⏳ Обрабатываю..."
            kb = [[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]
            if q:
                await q.edit_message_text(err_text, reply_markup=InlineKeyboardMarkup(kb))
            elif msg:
                await msg.reply_text(err_text, reply_markup=InlineKeyboardMarkup(kb))
        except Exception as e:
            _log.debug("Error handler: %s", e)
        return  # не пробрасываем — бот продолжает работать
    _log.exception("Handler error: %s", context.error)
    try:
        q = getattr(update, "callback_query", None)
        msg = getattr(update, "message", None)
        if q:
            await q.edit_message_text("⚠️ Ошибка. Нажмите «Меню» и попробуйте снова.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Меню", callback_data="main_menu")]]))
        elif msg:
            await msg.reply_text("⚠️ Ошибка. Попробуйте снова.")
    except Exception:
        pass
    return  # не крашим даже при других ошибках


def build_admin_app(token: str) -> Application:
    app = (
        Application.builder()
        .token(token)
        .updater(None)
        .concurrent_updates(1)  # 1 воркер — меньше нагрузка на пул БД при очереди
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .connection_pool_size(64)
        .build()
    )
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
    app = (
        Application.builder()
        .token(token)
        .updater(None)
        .concurrent_updates(1)  # 1 воркер — меньше нагрузка на пул БД при очереди
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .connection_pool_size(64)
        .build()
    )
    app.add_error_handler(_error_handler)
    app.add_handler(CommandHandler("start", client_start))
    app.add_handler(CommandHandler("mycode", client_mycode))
    app.add_handler(CallbackQueryHandler(client_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, client_buy))
    return app
