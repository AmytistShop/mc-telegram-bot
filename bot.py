# bot.py
# Aiogram 3.7+ compatible (NO parse_mode in Bot(...))
# Render-friendly: starts a tiny web server on PORT for uptime pings.

import os
import re
import json
import sqlite3
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties

# ==========================
# CONFIG
# ==========================

TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN.")

# Кто может управлять ботом (в ЛС и командами в группе)
ADMIN_IDS = {6911558950, 8085895186}

# Тег рекламы
AD_TAG = "#реклама"

# Раз в 24 часа (для тех, у кого есть разрешение)
AD_COOLDOWN_SECONDS = 24 * 60 * 60

# Наказания бота за рекламу (для тех, у кого НЕТ разрешения)
MUTE_2_SECONDS = 3 * 60 * 60   # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60  # 12 часов

# Бан за 4/3 предупреждения (ручные /mcwarn)
ADMIN_WARN_BAN_SECONDS = 3 * 24 * 60 * 60  # 3 дня

# Пагинация логов (как ты просил 5 на страницу)
LOGS_PAGE_SIZE = 5
LIST_PAGE_SIZE = 10

DB_PATH = "mc_bot.db"

# ==========================
# UTILS
# ==========================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_ts() -> int:
    return int(now_utc().timestamp())

def ts_to_str(ts: Optional[int]) -> str:
    if ts is None:
        return "навсегда"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
    return dt.strftime("%d.%m.%Y %H:%M")

def active_tag(ts: Optional[int]) -> str:
    if ts is None:
        return "[Активно]"
    return "[Активно]" if ts > now_ts() else "[Неактивно]"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )

def user_link(user_id: int, name: str, username: Optional[str]) -> str:
    # кликабельно как у ириса: по имени
    safe_name = escape_html(name or "Пользователь")
    if username:
        # Можно и на @username, но tg://user?id работает даже без юзернейма
        return f'<a href="https://t.me/{escape_html(username)}">{safe_name}</a>'
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'

def parse_duration(token: Optional[str]) -> Optional[int]:
    """
    Формат: 15m / 2h / 3d / 1w / 1y
    Если не указано -> None (навсегда)
    """
    if not token:
        return None
    t = token.strip().lower()
    m = re.fullmatch(r"(\d{1,6})([mhdwy])", t)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    mult = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
        "y": 365 * 24 * 60 * 60,
    }[unit]
    return n * mult

def hashtag_at_end(text: str) -> bool:
    return bool(re.search(r"#реклама\s*$", (text or "").lower()))

def has_hashtag(text: str) -> bool:
    return AD_TAG in (text or "").lower()

def contains_phone(text: str) -> bool:
    # грубо: +7..., 8..., 9xxxxxxxxx, или любые 10-12 цифр с разделителями
    t = text or ""
    return bool(re.search(r"(\+?\d[\d\-\s\(\)]{8,}\d)", t))

def contains_tg_link(text: str) -> bool:
    t = (text or "").lower()
    return ("t.me/" in t) or ("telegram.me/" in t) or ("@joinchat" in t) or ("joinchat" in t)

def looks_like_ad(text: str) -> Tuple[bool, Optional[str]]:
    """
    Критерии рекламы (как ты просил):
    - tg ссылки/инвайты
    - слова: сдам, продам, куплю, прайс, подпишитесь
    - номера телефонов
    username НЕ считать рекламой (просто @nick без t.me не триггерит)
    """
    t = (text or "").lower()

    keywords = ["сдам", "продам", "куплю", "прайс", "подпишитесь", "подписывайтесь"]
    for w in keywords:
        if re.search(rf"\b{re.escape(w)}\b", t):
            return True, f"ключевое слово: {w}"

    if contains_tg_link(text):
        return True, "ссылка Telegram"

    if contains_phone(text):
        return True, "номер телефона"

    return False, None

def msg_text_and_caption(msg: Message) -> str:
    # чтобы проверять и текст, и подпись к картинке/видео
    parts = []
    if msg.text:
        parts.append(msg.text)
    if msg.caption:
        parts.append(msg.caption)
    return "\n".join(parts).strip()

# ==========================
# DB
# ==========================

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS chats(
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            last_seen_ts INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_permits(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            until_ts INTEGER,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_last_sent(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            last_ts INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bot_ad_strikes(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manual_warns(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS sanctions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,         -- warn/mute/ban/kick/unwarn/unmute/unban
            until_ts INTEGER,
            reason TEXT,
            by_admin_id INTEGER,
            created_ts INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_deleted_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            msg_text TEXT,
            reason TEXT,
            created_ts INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS support_inbox(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER NOT NULL,
            from_username TEXT,
            from_name TEXT,
            text TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'open' -- open/closed
        )
    """)
    con.commit()
    return con

def chat_upsert(chat_id: int, title: str):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO chats(chat_id, title, last_seen_ts) VALUES (?,?,?)",
        (chat_id, title, now_ts())
    )
    con.commit()
    con.close()

def get_chats() -> List[sqlite3.Row]:
    con = db()
    cur = con.execute("SELECT chat_id, title, last_seen_ts FROM chats ORDER BY last_seen_ts DESC")
    rows = cur.fetchall()
    con.close()
    return rows

def permit_get(chat_id: int, user_id: int) -> Optional[int]:
    con = db()
    cur = con.execute("SELECT until_ts FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    return None if row is None else row["until_ts"]

def permit_active(chat_id: int, user_id: int) -> bool:
    until = permit_get(chat_id, user_id)
    if until is None:
        # может быть "навсегда", но отличаем: если записи нет -> нет разрешения
        con = db()
        cur = con.execute("SELECT 1 FROM ad_permits WHERE chat_id=? AND user_id=? LIMIT 1", (chat_id, user_id))
        ok = cur.fetchone() is not None
        con.close()
        return ok  # навсегда
    return until > now_ts()

def permit_set(chat_id: int, user_id: int, until_ts: Optional[int]):
    con = db()
    con.execute("INSERT OR REPLACE INTO ad_permits(chat_id,user_id,until_ts) VALUES (?,?,?)",
                (chat_id, user_id, until_ts))
    con.commit()
    con.close()

def permit_remove(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def last_ad_ts(chat_id: int, user_id: int) -> Optional[int]:
    con = db()
    cur = con.execute("SELECT last_ts FROM ad_last_sent WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    return None if row is None else row["last_ts"]

def set_last_ad_ts(chat_id: int, user_id: int, ts: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO ad_last_sent(chat_id,user_id,last_ts) VALUES (?,?,?)",
                (chat_id, user_id, ts))
    con.commit()
    con.close()

def bot_strikes_get(chat_id: int, user_id: int) -> int:
    con = db()
    cur = con.execute("SELECT strikes FROM bot_ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    return 0 if row is None else int(row["strikes"])

def bot_strikes_set(chat_id: int, user_id: int, strikes: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO bot_ad_strikes(chat_id,user_id,strikes) VALUES (?,?,?)",
                (chat_id, user_id, strikes))
    con.commit()
    con.close()

def bot_strikes_reset(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM bot_ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def manual_warns_get(chat_id: int, user_id: int) -> int:
    con = db()
    cur = con.execute("SELECT warns FROM manual_warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    return 0 if row is None else int(row["warns"])

def manual_warns_set(chat_id: int, user_id: int, warns: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO manual_warns(chat_id,user_id,warns) VALUES (?,?,?)",
                (chat_id, user_id, warns))
    con.commit()
    con.close()

def manual_warns_reset(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM manual_warns WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def log_sanction(chat_id: int, user_id: int, action: str, until_ts: Optional[int], reason: str, by_admin_id: int):
    con = db()
    con.execute("""
        INSERT INTO sanctions(chat_id,user_id,action,until_ts,reason,by_admin_id,created_ts)
        VALUES (?,?,?,?,?,?,?)
    """, (chat_id, user_id, action, until_ts, reason, by_admin_id, now_ts()))
    con.commit()
    con.close()

def log_deleted_ad(chat_id: int, user_id: int, username: Optional[str], full_name: str, msg_text: str, reason: str):
    con = db()
    con.execute("""
        INSERT INTO ad_deleted_logs(chat_id,user_id,username,full_name,msg_text,reason,created_ts)
        VALUES (?,?,?,?,?,?,?)
    """, (chat_id, user_id, username, full_name, msg_text[:2000], reason, now_ts()))
    con.commit()
    con.close()

def logs_page(chat_id: int, page: int) -> Tuple[List[sqlite3.Row], int]:
    con = db()
    cur_total = con.execute("SELECT COUNT(*) as c FROM ad_deleted_logs WHERE chat_id=?", (chat_id,))
    total = int(cur_total.fetchone()["c"])
    pages = max(1, (total + LOGS_PAGE_SIZE - 1) // LOGS_PAGE_SIZE)
    page = max(1, min(page, pages))
    offset = (page - 1) * LOGS_PAGE_SIZE
    cur = con.execute("""
        SELECT * FROM ad_deleted_logs
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (chat_id, LOGS_PAGE_SIZE, offset))
    rows = cur.fetchall()
    con.close()
    return rows, pages

def inbox_add(from_user_id: int, username: Optional[str], name: str, text: str) -> int:
    con = db()
    cur = con.execute("""
        INSERT INTO support_inbox(from_user_id,from_username,from_name,text,created_ts,status)
        VALUES (?,?,?,?,?, 'open')
    """, (from_user_id, username, name, text[:2000], now_ts()))
    con.commit()
    new_id = int(cur.lastrowid)
    con.close()
    return new_id

def inbox_list(page: int, page_size: int = 10) -> Tuple[List[sqlite3.Row], int]:
    con = db()
    cur_total = con.execute("SELECT COUNT(*) as c FROM support_inbox ORDER BY id DESC")
    total = int(cur_total.fetchone()["c"])
    pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, pages))
    offset = (page - 1) * page_size
    cur = con.execute("""
        SELECT * FROM support_inbox
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (page_size, offset))
    rows = cur.fetchall()
    con.close()
    return rows, pages

def inbox_get(item_id: int) -> Optional[sqlite3.Row]:
    con = db()
    cur = con.execute("SELECT * FROM support_inbox WHERE id=?", (item_id,))
    row = cur.fetchone()
    con.close()
    return row

# ==========================
# BOT / DISPATCHER
# ==========================

bot = Bot(
    TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()

# ==========================
# FSM
# ==========================

class PermitFlow(StatesGroup):
    pick_chat = State()
    pick_user = State()
    pick_duration = State()

class RemovePermitFlow(StatesGroup):
    pick_chat = State()
    pick_user = State()

class BroadcastFlow(StatesGroup):
    pick_chat = State()
    enter_text = State()

class SupportFlow(StatesGroup):
    enter_text = State()

class AdminReplyFlow(StatesGroup):
    enter_text = State()

# ==========================
# KEYBOARDS
# ==========================

def kb_main_menu(is_admin_user: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="Профиль", callback_data="menu:profile"),
            InlineKeyboardButton(text="Разрешения", callback_data="menu:perm"),
        ],
        [
            InlineKeyboardButton(text="Узнать ID", callback_data="menu:myid"),
            InlineKeyboardButton(text="Связаться с администратором", callback_data="menu:support"),
        ],
        [
            InlineKeyboardButton(text="VIP подписка", callback_data="menu:vip"),
        ],
    ]
    if is_admin_user:
        rows.insert(2, [InlineKeyboardButton(text="Логи", callback_data="menu:logs")])
        rows.insert(3, [InlineKeyboardButton(text="Рассылка", callback_data="menu:broadcast")])
        rows.insert(4, [InlineKeyboardButton(text="Сообщения", callback_data="menu:inbox")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(to: str = "menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=to)]
    ])

def kb_perm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выдать", callback_data="perm:give")],
        [InlineKeyboardButton(text="🗑️ Забрать", callback_data="perm:remove")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu:main")],
    ])

def kb_chat_pick(chats: List[sqlite3.Row], prefix: str) -> InlineKeyboardMarkup:
    # prefix: "givechat" / "rmchat" / "logschat" / "bcastchat"
    buttons = []
    for r in chats[:20]:
        title = r["title"] or str(r["chat_id"])
        buttons.append([InlineKeyboardButton(text=title, callback_data=f"{prefix}:{r['chat_id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_duration_pick() -> InlineKeyboardMarkup:
    # как ты хотел — выбор времени кнопками
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Навсегда", callback_data="dur:forever"),
            InlineKeyboardButton(text="15m", callback_data="dur:15m"),
            InlineKeyboardButton(text="1h", callback_data="dur:1h"),
        ],
        [
            InlineKeyboardButton(text="6h", callback_data="dur:6h"),
            InlineKeyboardButton(text="1d", callback_data="dur:1d"),
            InlineKeyboardButton(text="7d", callback_data="dur:7d"),
        ],
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:perm"),
        ]
    ])

def kb_logs_nav(chat_id: int, page: int, pages: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"logs:{chat_id}:{page-1}"))
    if page < pages:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"logs:{chat_id}:{page+1}"))
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_inbox_list_nav(page: int, pages: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"inbox:page:{page-1}"))
    if page < pages:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"inbox:page:{page+1}"))
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ==========================
# HELPERS: target user resolution
# ==========================

async def resolve_target_user(msg: Message) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Возвращает (user_id, username, display_name)
    Поддержка:
    - reply на сообщение: берет автора
    - forward: если есть forward_from
    - @username / id в тексте
    """
    # 1) reply
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        return u.id, u.username, u.full_name

    # 2) forward from user
    if msg.forward_from:
        u = msg.forward_from
        return u.id, u.username, u.full_name

    text = (msg.text or "").strip()

    # ищем @username
    m = re.search(r"@([A-Za-z0-9_]{5,32})", text)
    if m:
        # username -> id узнать нельзя напрямую без userbot/контакта.
        # Поэтому попросим переслать/ответить или указать ID
        return None, m.group(1), None

    # ищем id
    m2 = re.search(r"\b(\d{5,15})\b", text)
    if m2:
        return int(m2.group(1)), None, None

    return None, None, None

# ==========================
# TELEGRAM ACTIONS
# ==========================

async def do_mute(chat_id: int, user_id: int, seconds: Optional[int]):
    until = None
    if seconds is not None:
        until_dt = now_utc() + timedelta(seconds=seconds)
        until = int(until_dt.timestamp())
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions={"can_send_messages": False},
            until_date=until_dt,
        )
    else:
        # навсегда: until_date=None, но в TG мут навсегда делается очень большим until_date
        until_dt = now_utc() + timedelta(days=3650)
        until = int(until_dt.timestamp())
        await bot.restrict_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            permissions={"can_send_messages": False},
            until_date=until_dt,
        )
    return until

async def do_unmute(chat_id: int, user_id: int):
    await bot.restrict_chat_member(
        chat_id=chat_id,
        user_id=user_id,
        permissions={
            "can_send_messages": True,
            "can_send_audios": True,
            "can_send_documents": True,
            "can_send_photos": True,
            "can_send_videos": True,
            "can_send_video_notes": True,
            "can_send_voice_notes": True,
            "can_send_polls": True,
            "can_send_other_messages": True,
            "can_add_web_page_previews": True,
        },
    )

async def do_ban(chat_id: int, user_id: int, seconds: Optional[int]):
    until_ts = None
    if seconds is not None:
        until_dt = now_utc() + timedelta(seconds=seconds)
        until_ts = int(until_dt.timestamp())
        await bot.ban_chat_member(chat_id, user_id, until_date=until_dt)
    else:
        await bot.ban_chat_member(chat_id, user_id)
    return until_ts

async def do_unban(chat_id: int, user_id: int):
    await bot.unban_chat_member(chat_id, user_id)

async def do_kick(chat_id: int, user_id: int):
    # kick = ban then unban (быстро)
    await bot.ban_chat_member(chat_id, user_id)
    await bot.unban_chat_member(chat_id, user_id)

# ==========================
# COMMANDS: start/menu
# ==========================

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    if msg.chat.type != "private":
        return
    await msg.answer(
        "🏠 <b>MC Bot</b>\n"
        "Меню ниже 👇",
        reply_markup=kb_main_menu(is_admin(msg.from_user.id))
    )

@dp.callback_query(F.data == "menu:main")
async def cb_menu_main(q: CallbackQuery):
    await q.message.edit_text(
        "🏠 <b>MC Bot</b>\nМеню ниже 👇",
        reply_markup=kb_main_menu(is_admin(q.from_user.id))
    )
    await q.answer()

@dp.callback_query(F.data == "menu:vip")
async def cb_vip(q: CallbackQuery):
    await q.message.edit_text(
        "💎 <b>VIP подписка</b>\n\n"
        "Пока в разработке 🙂",
        reply_markup=kb_back("menu:main")
    )
    await q.answer()

@dp.callback_query(F.data == "menu:myid")
async def cb_myid(q: CallbackQuery):
    await q.message.edit_text(
        f"🪪 <b>Твой ID</b>: <code>{q.from_user.id}</code>",
        reply_markup=kb_back("menu:main")
    )
    await q.answer()

@dp.callback_query(F.data == "menu:profile")
async def cb_profile(q: CallbackQuery):
    status = "Админ" if is_admin(q.from_user.id) else "Участник"
    await q.message.edit_text(
        "👤 <b>Профиль</b>\n\n"
        f"Статус: <b>{status}</b>\n"
        f"ID: <code>{q.from_user.id}</code>\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")]
        ])
    )
    await q.answer()

# ==========================
# PERMISSIONS (admins only)
# ==========================

@dp.callback_query(F.data == "menu:perm")
async def cb_perm(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    await q.message.edit_text(
        "🔑 <b>Разрешения на рекламу</b>\n\n"
        "Выбери действие:",
        reply_markup=kb_perm_menu()
    )
    await q.answer()

@dp.callback_query(F.data == "perm:give")
async def cb_perm_give(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chats = get_chats()
    if not chats:
        await q.message.edit_text(
            "Нет чатов. Добавь бота в группу и напиши там любое сообщение, чтобы чат появился.",
            reply_markup=kb_back("menu:perm")
        )
        await q.answer()
        return
    await state.set_state(PermitFlow.pick_chat)
    await q.message.edit_text(
        "✅ <b>Выдать разрешение</b>\n\nВыбери чат:",
        reply_markup=kb_chat_pick(chats, "givechat")
    )
    await q.answer()

@dp.callback_query(F.data.startswith("givechat:"))
async def cb_givechat(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chat_id = int(q.data.split(":")[1])
    await state.update_data(chat_id=chat_id)
    await state.set_state(PermitFlow.pick_user)
    await q.message.edit_text(
        "Отправь:\n"
        "• <b>ID</b> пользователя\n"
        "• или <b>перешли</b> сообщение пользователя\n"
        "• или <b>ответь</b> на его сообщение\n"
        "• или напиши <b>@username</b> (но лучше ID/пересылка)\n\n"
        "⚠️ @username без ID может не определиться.",
        reply_markup=kb_back("menu:perm")
    )
    await q.answer()

@dp.message(PermitFlow.pick_user)
async def st_pick_user(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    chat_id = int(data["chat_id"])

    uid, uname, _ = await resolve_target_user(msg)

    if uid is None and uname:
        return await msg.answer(
            "Я вижу только @username, но <b>ID по нему не всегда могу получить</b>.\n"
            "Сделай так:\n"
            "1) попроси пользователя написать что-то в чат\n"
            "2) ответь на его сообщение и снова нажми выдачу\n"
            "или перешли его сообщение сюда.\n\n"
            "Либо введи <b>ID</b> цифрами.",
            reply_markup=kb_back("menu:perm")
        )

    if uid is None:
        return await msg.answer("Не смог определить пользователя. Пришли ID или пересылку/ответ.", reply_markup=kb_back("menu:perm"))

    await state.update_data(user_id=uid)
    await state.set_state(PermitFlow.pick_duration)
    await msg.answer(
        "Выбери срок разрешения:",
        reply_markup=kb_duration_pick()
    )

@dp.callback_query(PermitFlow.pick_duration, F.data.startswith("dur:"))
async def cb_pick_duration(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    data = await state.get_data()
    chat_id = int(data["chat_id"])
    user_id = int(data["user_id"])

    val = q.data.split(":")[1]
    if val == "forever":
        until = None
        permit_set(chat_id, user_id, until)
        await q.message.edit_text(
            f"✅ Готово.\nРазрешение выдано: <code>{user_id}</code>\nСрок: <b>навсегда</b>\n\n"
            "Можешь вернуться в меню.",
            reply_markup=kb_back("menu:perm")
        )
    else:
        seconds = parse_duration(val)
        until = now_ts() + int(seconds) if seconds else None
        permit_set(chat_id, user_id, until)
        await q.message.edit_text(
            f"✅ Готово.\nРазрешение выдано: <code>{user_id}</code>\n"
            f"До: <b>{ts_to_str(until)}</b> {active_tag(until)}\n\n"
            "Можешь вернуться в меню.",
            reply_markup=kb_back("menu:perm")
        )

    await state.clear()
    await q.answer()

@dp.callback_query(F.data == "perm:remove")
async def cb_perm_remove(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chats = get_chats()
    if not chats:
        await q.message.edit_text(
            "Нет чатов. Добавь бота в группу и напиши там любое сообщение, чтобы чат появился.",
            reply_markup=kb_back("menu:perm")
        )
        await q.answer()
        return
    await state.set_state(RemovePermitFlow.pick_chat)
    await q.message.edit_text(
        "🗑️ <b>Забрать разрешение</b>\n\nВыбери чат:",
        reply_markup=kb_chat_pick(chats, "rmchat")
    )
    await q.answer()

@dp.callback_query(RemovePermitFlow.pick_chat, F.data.startswith("rmchat:"))
async def cb_rmchat(q: CallbackQuery, state: FSMContext):
    chat_id = int(q.data.split(":")[1])
    await state.update_data(chat_id=chat_id)
    await state.set_state(RemovePermitFlow.pick_user)
    await q.message.edit_text(
        "Отправь ID / пересылку / ответ на сообщение пользователя.",
        reply_markup=kb_back("menu:perm")
    )
    await q.answer()

@dp.message(RemovePermitFlow.pick_user)
async def st_rm_user(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    chat_id = int(data["chat_id"])

    uid, uname, _ = await resolve_target_user(msg)
    if uid is None:
        return await msg.answer("Не смог определить пользователя. Пришли ID/пересылку/ответ.", reply_markup=kb_back("menu:perm"))

    permit_remove(chat_id, uid)
    await state.clear()
    await msg.answer(
        f"🗑️ Разрешение удалено: <code>{uid}</code>\n\n"
        "Можешь вернуться в меню.",
        reply_markup=kb_back("menu:perm")
    )

# ==========================
# LOGS (admins only)
# ==========================

@dp.callback_query(F.data == "menu:logs")
async def cb_logs(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chats = get_chats()
    if not chats:
        await q.message.edit_text(
            "Нет чатов. Добавь бота в группу и напиши там любое сообщение, чтобы чат появился.",
            reply_markup=kb_back("menu:main")
        )
        await q.answer()
        return
    await q.message.edit_text(
        "🧾 <b>Логи удалённых рекламных сообщений</b>\n\nВыбери чат:",
        reply_markup=kb_chat_pick(chats, "logschat")
    )
    await q.answer()

@dp.callback_query(F.data.startswith("logschat:"))
async def cb_logs_chat(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chat_id = int(q.data.split(":")[1])
    rows, pages = logs_page(chat_id, 1)
    text = render_logs(chat_id, 1, pages, rows)
    await q.message.edit_text(text, reply_markup=kb_logs_nav(chat_id, 1, pages))
    await q.answer()

@dp.callback_query(F.data.startswith("logs:"))
async def cb_logs_page(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    _, chat_id_s, page_s = q.data.split(":")
    chat_id = int(chat_id_s)
    page = int(page_s)
    rows, pages = logs_page(chat_id, page)
    text = render_logs(chat_id, page, pages, rows)
    await q.message.edit_text(text, reply_markup=kb_logs_nav(chat_id, page, pages))
    await q.answer()

def render_logs(chat_id: int, page: int, pages: int, rows: List[sqlite3.Row]) -> str:
    out = [f"🧾 <b>Логи</b> (чат <code>{chat_id}</code>)\nСтраница {page}/{pages}\n"]
    if not rows:
        out.append("Пока нет логов.")
        return "\n".join(out)
    for r in rows:
        dt = ts_to_str(int(r["created_ts"]))
        uid = int(r["user_id"])
        uname = r["username"]
        name = r["full_name"] or "Пользователь"
        reason = r["reason"] or "-"
        msg_text = (r["msg_text"] or "").strip()
        if len(msg_text) > 200:
            msg_text = msg_text[:200] + "…"
        out.append(
            "━━━━━━━━━━━━━━\n"
            f"👤 {user_link(uid, name, uname)} (<code>{uid}</code>)\n"
            f"🕒 {dt}\n"
            f"❗ Причина: <b>{escape_html(reason)}</b>\n"
            f"📝 Текст: <code>{escape_html(msg_text)}</code>"
        )
    return "\n".join(out)

# ==========================
# BROADCAST (admins only)
# ==========================

@dp.callback_query(F.data == "menu:broadcast")
async def cb_broadcast(q: CallbackQuery, state: FSMContext):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    chats = get_chats()
    if not chats:
        await q.message.edit_text(
            "Нет чатов. Добавь бота в группу и напиши там любое сообщение, чтобы чат появился.",
            reply_markup=kb_back("menu:main")
        )
        await q.answer()
        return
    await state.set_state(BroadcastFlow.pick_chat)
    await q.message.edit_text(
        "📣 <b>Рассылка</b>\n\nВыбери чат:",
        reply_markup=kb_chat_pick(chats, "bcastchat")
    )
    await q.answer()

@dp.callback_query(BroadcastFlow.pick_chat, F.data.startswith("bcastchat:"))
async def cb_bcast_chat(q: CallbackQuery, state: FSMContext):
    chat_id = int(q.data.split(":")[1])
    await state.update_data(chat_id=chat_id)
    await state.set_state(BroadcastFlow.enter_text)
    await q.message.edit_text(
        "Отправь текст рассылки одним сообщением.",
        reply_markup=kb_back("menu:main")
    )
    await q.answer()

@dp.message(BroadcastFlow.enter_text)
async def st_bcast_text(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    chat_id = int(data["chat_id"])
    text = msg_text_and_caption(msg)
    if not text:
        return await msg.answer("Пришли текст.")
    await bot.send_message(chat_id, text)
    await state.clear()
    await msg.answer("✅ Рассылка отправлена.", reply_markup=kb_back("menu:main"))

# ==========================
# SUPPORT (all users -> admins)
# ==========================

@dp.callback_query(F.data == "menu:support")
async def cb_support(q: CallbackQuery, state: FSMContext):
    await state.set_state(SupportFlow.enter_text)
    await q.message.edit_text(
        "🆘 <b>Связаться с администратором</b>\n\n"
        "Напиши сообщение — я передам админам.",
        reply_markup=kb_back("menu:main")
    )
    await q.answer()

@dp.message(SupportFlow.enter_text)
async def st_support_send(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    text = msg_text_and_caption(msg)
    if not text:
        return await msg.answer("Пришли текст.")
    item_id = inbox_add(msg.from_user.id, msg.from_user.username, msg.from_user.full_name, text)

    # отправим всем админам
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(
                aid,
                "📩 <b>Новое сообщение в поддержку</b>\n\n"
                f"ID заявки: <code>{item_id}</code>\n"
                f"От: {user_link(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)} "
                f"(<code>{msg.from_user.id}</code>)\n\n"
                f"Текст:\n<code>{escape_html(text)}</code>\n\n"
                "Чтобы ответить: /reply <id_заявки> <текст>",
            )
        except Exception:
            pass

    await state.clear()
    await msg.answer("✅ Сообщение отправлено администраторам.", reply_markup=kb_main_menu(is_admin(msg.from_user.id)))

@dp.callback_query(F.data == "menu:inbox")
async def cb_inbox(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    rows, pages = inbox_list(page=1, page_size=10)
    await q.message.edit_text(render_inbox(rows, 1, pages), reply_markup=kb_inbox_list_nav(1, pages))
    await q.answer()

@dp.callback_query(F.data.startswith("inbox:page:"))
async def cb_inbox_page(q: CallbackQuery):
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return
    page = int(q.data.split(":")[2])
    rows, pages = inbox_list(page=page, page_size=10)
    await q.message.edit_text(render_inbox(rows, page, pages), reply_markup=kb_inbox_list_nav(page, pages))
    await q.answer()

def render_inbox(rows: List[sqlite3.Row], page: int, pages: int) -> str:
    out = [f"📨 <b>Сообщения</b>\nСтраница {page}/{pages}\n"]
    if not rows:
        out.append("Пока пусто.")
        return "\n".join(out)
    for r in rows[:10]:
        dt = ts_to_str(int(r["created_ts"]))
        uid = int(r["from_user_id"])
        uname = r["from_username"]
        name = r["from_name"] or "Пользователь"
        text = r["text"] or ""
        if len(text) > 140:
            text = text[:140] + "…"
        out.append(
            "━━━━━━━━━━━━━━\n"
            f"ID: <code>{r['id']}</code>\n"
            f"От: {user_link(uid, name, uname)} (<code>{uid}</code>)\n"
            f"🕒 {dt}\n"
            f"📝 <code>{escape_html(text)}</code>"
        )
    out.append("\nОтветить: <code>/reply ID текст</code>")
    return "\n".join(out)

@dp.message(Command("reply"))
async def cmd_reply(msg: Message):
    # /reply <ticket_id> <text>
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        return await msg.answer("Использование: /reply <id_заявки> <текст>")
    ticket_id = int(parts[1])
    text = parts[2].strip()
    row = inbox_get(ticket_id)
    if not row:
        return await msg.answer("Заявка не найдена.")
    to_uid = int(row["from_user_id"])
    try:
        await bot.send_message(to_uid, f"💬 <b>Ответ администратора</b>\n\n<code>{escape_html(text)}</code>")
        await msg.answer("✅ Отправлено.")
    except Exception as e:
        await msg.answer(f"❌ Не удалось отправить: {e}")

# ==========================
# GROUP CHAT TRACKING
# ==========================

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def track_chat(msg: Message):
    # чтобы чат появлялся в списке (после перезапуска тоже)
    title = msg.chat.title or str(msg.chat.id)
    chat_upsert(msg.chat.id, title)

# ==========================
# MODERATION: advertising system
# ==========================

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def ad_watcher(msg: Message):
    # сначала пропускаем сервисные типы/пустые
    full_text = msg_text_and_caption(msg)
    if not full_text:
        return

    chat_id = msg.chat.id
    uid = msg.from_user.id

    # определяем реклама или нет
    is_ad, why = looks_like_ad(full_text)

    if not is_ad:
        return

    # если есть разрешение
    if permit_active(chat_id, uid):
        # 1) проверка тега
        if not hashtag_at_end(full_text):
            try:
                await msg.delete()
            except Exception:
                pass
            log_deleted_ad(chat_id, uid, msg.from_user.username, msg.from_user.full_name, full_text, "нет тега #реклама в конце")
            await bot.send_message(
                chat_id,
                "🗑️ Ваше сообщение удалено, по причине отсутствия тега на рекламу.\n"
                f'Пожалуйста укажите в вашей рекламе тег "<b>{AD_TAG}</b>".'
            )
            return

        # 2) кулдаун 24h
        last = last_ad_ts(chat_id, uid)
        if last and (now_ts() - last) < AD_COOLDOWN_SECONDS:
            try:
                await msg.delete()
            except Exception:
                pass
            left = AD_COOLDOWN_SECONDS - (now_ts() - last)
            hours = max(1, int(left // 3600))
            log_deleted_ad(chat_id, uid, msg.from_user.username, msg.from_user.full_name, full_text, f"кулдаун рекламы (осталось ~{hours}ч)")
            await bot.send_message(
                chat_id,
                f"⏳ Рекламу можно отправлять <b>раз в 24 часа</b>.\n"
                f"Подождите примерно <b>{hours} ч</b>."
            )
            return

        # можно отправлять
        set_last_ad_ts(chat_id, uid, now_ts())
        return

    # НЕТ разрешения
    # Если человек попытался писать с #реклама — ты просил: удалять и писать "нет разрешения" (без наказаний по тегу)
    if has_hashtag(full_text):
        try:
            await msg.delete()
        except Exception:
            pass
        log_deleted_ad(chat_id, uid, msg.from_user.username, msg.from_user.full_name, full_text, "нет разрешения на #реклама")
        await bot.send_message(
            chat_id,
            "⛔ У вас <b>нет разрешения</b> на рекламу.\n"
            "Получить разрешение на рекламу можете в тех.поддержке: @minecraft_chat_igra_bot"
        )
        return

    # реклама без разрешения: удаляем + стадийные наказания бота (warn -> mute 3h -> mute 12h + сброс)
    try:
        await msg.delete()
    except Exception:
        pass
    reason = why or "реклама"
    log_deleted_ad(chat_id, uid, msg.from_user.username, msg.from_user.full_name, full_text, reason)

    strikes = bot_strikes_get(chat_id, uid) + 1
    bot_strikes_set(chat_id, uid, strikes)

    who = user_link(uid, msg.from_user.full_name, msg.from_user.username)
    rules_link = "https://leoned777.github.io/chats/"

    if strikes == 1:
        await bot.send_message(
            chat_id,
            f"⚠️ {who}, <b>предупреждение</b> за рекламу без разрешения.\n"
            f"Причина: <b>{escape_html(reason)}</b>\n\n"
            f"Ознакомится с правилами можете тут -> {rules_link}"
        )
    elif strikes == 2:
        until = await do_mute(chat_id, uid, MUTE_2_SECONDS)
        await bot.send_message(
            chat_id,
            f"🔇 {who}, мут на <b>3 часа</b> за рекламу без разрешения.\n"
            f"Причина: <b>{escape_html(reason)}</b>\n\n"
            f"Ознакомится с правилами можете тут -> {rules_link}"
        )
    else:
        # 3-я стадия: мут 12ч + сброс предупреждений бота
        await do_mute(chat_id, uid, MUTE_3_SECONDS)
        bot_strikes_reset(chat_id, uid)
        await bot.send_message(
            chat_id,
            f"🔇 {who}, мут на <b>12 часов</b> за рекламу без разрешения.\n"
            f"Причина: <b>{escape_html(reason)}</b>\n"
            f"Страйки сброшены.\n\n"
            f"Ознакомится с правилами можете тут -> {rules_link}"
        )

# ==========================
# MC ADMIN COMMANDS (group & private) - respond only if sender is in ADMIN_IDS
# ==========================

def parse_mc_command(msg: Message) -> Tuple[Optional[str], Optional[int], Optional[int], str]:
    """
    Возвращает (action, target_user_id, duration_seconds, reason)
    Форматы:
      /mcban @user 1d причина
      /mcban 1d причина  (если ответом на сообщение)
      /mckick причина
    """
    text = (msg.text or "").strip()
    parts = text.split()
    cmd = parts[0].lower()

    action_map = {
        "/mcwarn": "warn",
        "/mcunwarn": "unwarn",
        "/mcmute": "mute",
        "/mcunmute": "unmute",
        "/mcban": "ban",
        "/mcunban": "unban",
        "/mckick": "kick",
    }
    action = action_map.get(cmd)
    if not action:
        return None, None, None, ""

    # target (reply preferred)
    target_id = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = msg.reply_to_message.from_user.id

    # try parse @ or id
    dur = None
    reason = ""

    # Считаем аргументы без команды
    args = parts[1:]

    # Если первый аргумент похож на @user или id — пропускаем (но username->id не всегда)
    if args:
        if args[0].startswith("@"):
            # username без id: нельзя гарантированно применить
            # но если есть reply — применим к reply, иначе ошибка
            if target_id is None:
                return action, None, None, "Нужно ответить на сообщение пользователя или указать его ID цифрами."
            args = args[1:]
        elif re.fullmatch(r"\d{5,15}", args[0]):
            target_id = int(args[0])
            args = args[1:]

    # duration: если next token = 15m/2h/3d/...
    if args:
        d = parse_duration(args[0])
        if d is not None:
            dur = d
            args = args[1:]

    # reason: остаток
    reason = " ".join(args).strip() if args else ""
    if not reason:
        reason = "причина не указана"

    return action, target_id, dur, reason

def mc_help_tail() -> str:
    return (
        "\n\n<i>Подсказка:</i>\n"
        "• Ответом на сообщение: <code>/mcmute 1h причина</code>\n"
        "• По ID: <code>/mcban 1d 123456789 причина</code>\n"
        "• Или: <code>/mcwarn @user причина</code> (лучше ответом)\n"
    )

@dp.message(Command("mclist"))
async def cmd_mclist(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    # доступно всем
    page = 1
    parts = (msg.text or "").split()
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))
    text, pages = render_mclist(msg.chat.id, page)
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"mclist:{msg.chat.id}:{page-1}"))
    if page < pages:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"mclist:{msg.chat.id}:{page+1}"))
    rows = []
    if nav:
        rows.append(nav)
    kb.inline_keyboard = rows
    await msg.reply(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("mclist:"))
async def cb_mclist(q: CallbackQuery):
    _, chat_id_s, page_s = q.data.split(":")
    chat_id = int(chat_id_s)
    page = int(page_s)
    text, pages = render_mclist(chat_id, page)
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"mclist:{chat_id}:{page-1}"))
    if page < pages:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"mclist:{chat_id}:{page+1}"))
    kb = InlineKeyboardMarkup(inline_keyboard=[nav] if nav else [])
    await q.message.edit_text(text, reply_markup=kb)
    await q.answer()

def render_mclist(chat_id: int, page: int) -> Tuple[str, int]:
    con = db()
    cur_total = con.execute("SELECT COUNT(*) as c FROM sanctions WHERE chat_id=?", (chat_id,))
    total = int(cur_total.fetchone()["c"])
    pages = max(1, (total + LIST_PAGE_SIZE - 1) // LIST_PAGE_SIZE)
    page = max(1, min(page, pages))
    offset = (page - 1) * LIST_PAGE_SIZE
    cur = con.execute("""
        SELECT * FROM sanctions
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (chat_id, LIST_PAGE_SIZE, offset))
    rows = cur.fetchall()
    con.close()

    out = [f"📄 <b>MC List</b> (10 последних) — страница {page}/{pages}\n"]
    if not rows:
        out.append("Пока пусто.")
        return "\n".join(out), pages
    for r in rows:
        uid = int(r["user_id"])
        action = r["action"]
        until_ts = r["until_ts"]
        reason = r["reason"] or "-"
        created = ts_to_str(int(r["created_ts"]))
        out.append(
            "━━━━━━━━━━━━━━\n"
            f"👤 <code>{uid}</code>\n"
            f"⚙️ {escape_html(action)} до: <b>{ts_to_str(until_ts)}</b> {active_tag(until_ts)}\n"
            f"🕒 выдано: {created}\n"
            f"📝 причина: <code>{escape_html(reason)}</code>"
        )
    out.append("\n<i>Чтобы продолжить:</i> кнопки ниже или <code>/mclist 2</code>")
    return "\n".join(out), pages

# обработка всех /mc... команд
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/mc"))
async def mc_commands(msg: Message):
    if not is_admin(msg.from_user.id):
        return  # команды могут писать все, но реагируем только на ADMIN_IDS

    action, target_id, dur, reason = parse_mc_command(msg)
    if action is None:
        return

    if target_id is None:
        return await msg.reply("Не могу определить пользователя. Ответь на сообщение или укажи ID.", disable_web_page_preview=True)

    chat_id = msg.chat.id
    who_admin = msg.from_user.id

    # имя для сообщения
    target_name = msg.reply_to_message.from_user.full_name if (msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.id == target_id) else "Пользователь"
    target_username = msg.reply_to_message.from_user.username if (msg.reply_to_message and msg.reply_to_message.from_user and msg.reply_to_message.from_user.id == target_id) else None
    who = user_link(target_id, target_name, target_username)

    # WARN / UNWARN (ручные)
    if action == "warn":
        warns = manual_warns_get(chat_id, target_id) + 1
        manual_warns_set(chat_id, target_id, warns)

        # 4/3 -> бан 3 дня и сброс
        if warns >= 4:
            until_ts = await do_ban(chat_id, target_id, ADMIN_WARN_BAN_SECONDS)
            log_sanction(chat_id, target_id, "ban", until_ts, f"бан за предупреждения (4/3). {reason}", who_admin)
            manual_warns_reset(chat_id, target_id)
            await msg.reply(
                f"⛔ {who} получил <b>БАН на 3 дня</b> (предупреждения: <b>4/3</b>).\n"
                f"Причина: <code>{escape_html(reason)}</code>"
                + mc_help_tail()
            )
            return

        log_sanction(chat_id, target_id, "warn", None, reason, who_admin)
        await msg.reply(
            f"⚠️ {who} получил предупреждение <b>{warns}/3</b>.\n"
            f"Причина: <code>{escape_html(reason)}</code>"
            + mc_help_tail()
        )
        return

    if action == "unwarn":
        manual_warns_reset(chat_id, target_id)
        log_sanction(chat_id, target_id, "unwarn", None, reason, who_admin)
        await msg.reply(
            f"✅ {who} предупреждения сброшены.\n"
            f"Причина: <code>{escape_html(reason)}</code>"
        )
        return

    # MUTE
    if action == "mute":
        until_ts = await do_mute(chat_id, target_id, dur)
        log_sanction(chat_id, target_id, "mute", until_ts, reason, who_admin)
        await msg.reply(
            f"🔇 {who} мут.\n"
            f"До: <b>{ts_to_str(until_ts)}</b> {active_tag(until_ts)}\n"
            f"Причина: <code>{escape_html(reason)}</code>"
            + mc_help_tail()
        )
        return

    if action == "unmute":
        await do_unmute(chat_id, target_id)
        log_sanction(chat_id, target_id, "unmute", None, reason, who_admin)
        await msg.reply(
            f"✅ {who} размучен.\n"
            f"Причина: <code>{escape_html(reason)}</code>"
        )
        return

    # BAN
    if action == "ban":
        until_ts = await do_ban(chat_id, target_id, dur)
        log_sanction(chat_id, target_id, "ban", until_ts, reason, who_admin)
        await msg.reply(
            f"⛔ {who} бан.\n"
            f"До: <b>{ts_to_str(until_ts)}</b> {active_tag(until_ts)}\n"
            f"Причина: <code>{escape_html(reason)}</code>"
            + mc_help_tail()
        )
        return

    if action == "unban":
        await do_unban(chat_id, target_id)
        log_sanction(chat_id, target_id, "unban", None, reason, who_admin)
        await msg.reply(
            f"✅ {who} разбанен.\n"
            f"Причина: <code>{escape_html(reason)}</code>"
        )
        return

    # KICK
    if action == "kick":
        await do_kick(chat_id, target_id)
        log_sanction(chat_id, target_id, "kick", None, reason, who_admin)
        await msg.reply(
            f"👢 {who} кик.\n"
            f"Причина: <code>{escape_html(reason)}</code>"
            + mc_help_tail()
        )
        return

# ==========================
# WEB SERVER for Render + UptimeRobot
# ==========================

async def handle_root(request: web.Request):
    return web.Response(text="OK")

async def start_web():
    app = web.Application()
    app.router.add_get("/", handle_root)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[web] listening on 0.0.0.0:{port}")

# ==========================
# MAIN
# ==========================

async def main():
    db().close()
    await start_web()
    print("[bot] starting polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
