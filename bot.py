import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats,
)
from aiogram.exceptions import TelegramBadRequest


# =========================
# НАСТРОЙКИ
# =========================
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN.")

# Админы (ты дал). Добавил твой id.
ADMIN_IDS = {8085895186, 6911558950}

DB_PATH = "mc_bot.db"

HASHTAG = "#реклама"
RULES_LINK = "https://leoned777.github.io/chats/"
SUPPORT_BOT = "@minecraft_chat_igra_bot"

# анти-реклама ключи (можешь дополнять)
AD_KEYWORDS = [
    "сдам", "продам", "куплю", "прайс", "подпишитесь", "подпишись",
]

# муты за рекламу (стадии бота)
MUTE_STAGE_2 = 3 * 60 * 60   # 3 часа
MUTE_STAGE_3 = 12 * 60 * 60  # 12 часов

LOGS_PAGE_SIZE = 5
LIST_PAGE_SIZE = 10

ADS_COOLDOWN_SECONDS = 24 * 60 * 60  # 24 часа


# =========================
# УТИЛИТЫ
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def dt_to_str_local(ts: Optional[int]) -> str:
    if ts is None:
        return "Навсегда"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
    return dt.strftime("%d.%m.%Y %H:%M")

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def user_link_html(user_id: int, full_name: str, username: Optional[str]) -> str:
    name = html_escape(full_name or "Пользователь")
    if username:
        return f'<a href="https://t.me/{username}">{name}</a>'
    return f'<a href="tg://user?id={user_id}">{name}</a>'

def normalize_text(text: Optional[str]) -> str:
    return (text or "").strip()

def parse_duration(token: Optional[str]) -> Optional[int]:
    """
    '15m' '2h' '3d' '1w' '1y' -> seconds
    None -> None (навсегда)
    """
    if not token:
        return None
    t = token.strip().lower()
    m = re.fullmatch(r"(\d{1,6})([mhdwy])", t)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    mult = {"m": 60, "h": 3600, "d": 86400, "w": 604800, "y": 31536000}[unit]
    return n * mult

def extract_telegram_links(text: str) -> bool:
    t = text.lower()
    # t.me/..., telegram.me/..., @joinchat, invite links
    return bool(re.search(r"(t\.me\/|telegram\.me\/|joinchat\/|t\.me\+)", t))

def extract_phone(text: str) -> bool:
    # очень грубо: 9+ цифр подряд или +7..., 8...
    return bool(re.search(r"(\+?\d[\d\-\s\(\)]{8,}\d)", text))

def contains_ad(text: str) -> Tuple[bool, str]:
    """
    Возвращает (is_ad, reason_keyword)
    username @xxx НЕ считаем рекламой сам по себе
    """
    t = (text or "").lower()

    # не считать просто @username рекламой
    # но если вместе с другими признаками — тогда реклама
    has_link = extract_telegram_links(t)
    has_phone = extract_phone(t)

    for kw in AD_KEYWORDS:
        if kw in t:
            return True, kw

    if has_link:
        return True, "ссылка"
    if has_phone:
        return True, "телефон"

    return False, ""

def has_hashtag_anywhere(text: str) -> bool:
    return HASHTAG in (text or "").lower()

def hashtag_at_end(text: str) -> bool:
    return bool(re.search(r"#реклама\s*$", (text or "").lower()))


# =========================
# БАЗА ДАННЫХ
# =========================
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            expires_at INTEGER,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_last_sent (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            last_ts INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_strikes (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS deleted_ads_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            content TEXT,
            reason TEXT,
            keyword TEXT,
            created_at INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS chats_seen (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            last_seen INTEGER NOT NULL
        )
    """)

    # ручные наказания
    con.execute("""
        CREATE TABLE IF NOT EXISTS manual_punishments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            ptype TEXT NOT NULL,         -- warn/mute/ban/kick
            issued_by INTEGER NOT NULL,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER,
            reason TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS manual_warn_counter (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    # чат поддержки
    con.execute("""
        CREATE TABLE IF NOT EXISTS support_msgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL,      -- open/closed
            reply_to INTEGER,
            replied_by INTEGER,
            reply_text TEXT,
            replied_at INTEGER
        )
    """)

    con.commit()
    return con

def touch_chat(chat_id: int, title: str):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO chats_seen(chat_id, title, last_seen) VALUES (?,?,?)",
        (chat_id, title, int(utcnow().timestamp()))
    )
    con.commit()
    con.close()

def permit_get(chat_id: int, user_id: int) -> Optional[int]:
    con = db()
    cur = con.execute(
        "SELECT expires_at FROM permits WHERE chat_id=? AND user_id=?",
        (chat_id, user_id)
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return row[0]

def permit_is_active(chat_id: int, user_id: int) -> bool:
    exp = permit_get(chat_id, user_id)
    if exp is None:
        return False
    if exp == 0:
        return True
    return exp > int(utcnow().timestamp())

def permit_set(chat_id: int, user_id: int, seconds: Optional[int]):
    exp = 0
    if seconds is not None:
        exp = int((utcnow() + timedelta(seconds=seconds)).timestamp())
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO permits(chat_id, user_id, expires_at) VALUES (?,?,?)",
        (chat_id, user_id, exp)
    )
    con.commit()
    con.close()

def permit_remove(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def ad_cooldown_left(chat_id: int, user_id: int) -> int:
    con = db()
    cur = con.execute("SELECT last_ts FROM ad_last_sent WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    if not row:
        return 0
    last_ts = int(row[0])
    now_ts = int(utcnow().timestamp())
    left = (last_ts + ADS_COOLDOWN_SECONDS) - now_ts
    return max(0, left)

def ad_cooldown_mark(chat_id: int, user_id: int):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO ad_last_sent(chat_id,user_id,last_ts) VALUES (?,?,?)",
        (chat_id, user_id, int(utcnow().timestamp()))
    )
    con.commit()
    con.close()

def strikes_get(chat_id: int, user_id: int) -> int:
    con = db()
    cur = con.execute("SELECT strikes FROM ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    con.close()
    return int(row[0]) if row else 0

def strikes_set(chat_id: int, user_id: int, n: int):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO ad_strikes(chat_id,user_id,strikes) VALUES (?,?,?)",
        (chat_id, user_id, int(n))
    )
    con.commit()
    con.close()

def strikes_reset(chat_id: int, user_id: int):
    strikes_set(chat_id, user_id, 0)

def log_deleted_ad(chat_id: int, msg: Message, content: str, reason: str, keyword: str):
    con = db()
    con.execute("""
        INSERT INTO deleted_ads_log(chat_id,user_id,username,full_name,content,reason,keyword,created_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        chat_id,
        msg.from_user.id,
        msg.from_user.username,
        msg.from_user.full_name,
        content[:4000],
        reason,
        keyword,
        int(utcnow().timestamp())
    ))
    con.commit()
    con.close()


# =========================
# КЛАВИАТУРЫ (ЛС)
# =========================
def kb_main(isadm: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="Профиль", callback_data="menu:profile"),
            InlineKeyboardButton(text="Разрешения", callback_data="menu:perm"),
        ],
        [
            InlineKeyboardButton(text="Узнать ID", callback_data="menu:myid"),
            InlineKeyboardButton(text="Связаться с администратором", callback_data="menu:support"),
        ],
    ]
    if isadm:
        rows.insert(2, [
            InlineKeyboardButton(text="Логи", callback_data="menu:logs"),
            InlineKeyboardButton(text="Сообщения", callback_data="menu:inbox"),
        ])
        rows.insert(3, [
            InlineKeyboardButton(text="Рассылка", callback_data="menu:broadcast"),
        ])
    rows.append([InlineKeyboardButton(text="VIP подписка", callback_data="menu:vip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(to: str = "menu:main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=to)]
    ])

def kb_perm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выдать", callback_data="perm:give")],
        [InlineKeyboardButton(text="🗑️ Забрать", callback_data="perm:remove")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])

def kb_logs(chat_buttons: List[Tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chat_buttons[:10]:
        rows.append([InlineKeyboardButton(text=title, callback_data=f"logs:chat:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_logs_pager(chat_id: int, page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"logs:page:{chat_id}:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"logs:page:{chat_id}:{page+1}"))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_broadcast_choose_chat(chat_buttons: List[Tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chat_buttons[:15]:
        rows.append([InlineKeyboardButton(text=title, callback_data=f"bc:chat:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_inbox(chat_buttons: List[Tuple[int, str]]) -> InlineKeyboardMarkup:
    # в inbox показываем "Все обращения" (без выбора чата) + назад
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Открытые обращения", callback_data="inbox:list:0")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])

def kb_inbox_pager(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"inbox:list:{page-1}"))
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"inbox:list:{page+1}"))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="🏠 Меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# БОТ
# =========================
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# =========================
# КОМАНДЫ "как у ириса" (/)
# =========================
async def setup_commands():
    # общие
    private_cmds = [
        BotCommand(command="start", description="Меню бота"),
        BotCommand(command="myid", description="Узнать свой ID"),
        BotCommand(command="adgive", description="Выдать разрешение на рекламу"),
        BotCommand(command="adremove", description="Забрать разрешение на рекламу"),
    ]
    group_cmds = [
        BotCommand(command="chatid", description="Показать ID чата"),
        BotCommand(command="mclist", description="Список наказаний (10 последних)"),
        BotCommand(command="mcwarn", description="Предупреждение"),
        BotCommand(command="mcmute", description="Мут"),
        BotCommand(command="mckick", description="Кик"),
        BotCommand(command="mcban", description="Бан"),
        BotCommand(command="mcunwarn", description="Снять предупреждения"),
        BotCommand(command="mcunmute", description="Снять мут"),
        BotCommand(command="mcunlock", description="Снять мут (алиас)"),
        BotCommand(command="mcunban", description="Снять бан"),
        BotCommand(command="adgive", description="Выдать разрешение на рекламу"),
        BotCommand(command="adremove", description="Забрать разрешение на рекламу"),
    ]
    await bot.set_my_commands(private_cmds, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(group_cmds, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(private_cmds, scope=BotCommandScopeDefault())


# =========================
# /start + меню в ЛС
# =========================
@dp.message(CommandStart())
async def cmd_start(msg: Message):
    if msg.chat.type == ChatType.PRIVATE:
        await msg.answer(
            "👋 Привет! Это бот модерации.\n"
            "Выбери действие в меню ниже 👇",
            reply_markup=kb_main(is_admin(msg.from_user.id))
        )

@dp.callback_query(F.data == "menu:main")
async def menu_main(cq: CallbackQuery):
    await cq.message.edit_text(
        "🏠 Главное меню",
        reply_markup=kb_main(is_admin(cq.from_user.id))
    )
    await cq.answer()

@dp.callback_query(F.data == "menu:profile")
async def menu_profile(cq: CallbackQuery):
    status = "Админ" if is_admin(cq.from_user.id) else "Участник"
    await cq.message.edit_text(
        f"👤 <b>Профиль</b>\n\n"
        f"Статус: <b>{status}</b>\n"
        f"ID: <code>{cq.from_user.id}</code>\n",
        reply_markup=kb_back("menu:main")
    )
    await cq.answer()

@dp.callback_query(F.data == "menu:myid")
async def menu_myid(cq: CallbackQuery):
    await cq.message.edit_text(
        f"🆔 Твой Telegram ID: <code>{cq.from_user.id}</code>",
        reply_markup=kb_back("menu:main")
    )
    await cq.answer()

@dp.callback_query(F.data == "menu:vip")
async def menu_vip(cq: CallbackQuery):
    await cq.message.edit_text(
        "⭐ <b>VIP подписка</b>\n\n"
        "Пока что в разработке 🙂",
        reply_markup=kb_back("menu:main")
    )
    await cq.answer()


# =========================
# Разрешения (меню)
# =========================
@dp.callback_query(F.data == "menu:perm")
async def menu_perm(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.message.edit_text("❌ Нет доступа.", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    await cq.message.edit_text(
        "🔑 <b>Разрешения на рекламу</b>\n\n"
        "Можно выдавать по @username, по ID или пересылкой сообщения.",
        reply_markup=kb_perm()
    )
    await cq.answer()

# режимы ввода (простая реализация через "ожидание" в sqlite не делаем — проще: просим командой)
@dp.callback_query(F.data == "perm:give")
async def perm_give_hint(cq: CallbackQuery):
    await cq.message.edit_text(
        "✅ <b>Выдать разрешение</b>\n\n"
        "Способы:\n"
        "1) Командой: <code>/adgive @user 15m</code>\n"
        "2) Командой: <code>/adgive 123456789 1d</code>\n"
        "3) Переслать сюда сообщение пользователя и написать: <code>/adgive 1d</code>\n\n"
        "Если время не указать — навсегда.",
        reply_markup=kb_back("menu:perm")
    )
    await cq.answer()

@dp.callback_query(F.data == "perm:remove")
async def perm_remove_hint(cq: CallbackQuery):
    await cq.message.edit_text(
        "🗑️ <b>Забрать разрешение</b>\n\n"
        "Способы:\n"
        "1) <code>/adremove @user</code>\n"
        "2) <code>/adremove 123456789</code>\n"
        "3) Переслать сюда сообщение пользователя и написать: <code>/adremove</code>",
        reply_markup=kb_back("menu:perm")
    )
    await cq.answer()


# =========================
# Логи (меню)
# =========================
def get_seen_chats() -> List[Tuple[int, str]]:
    con = db()
    cur = con.execute("SELECT chat_id, COALESCE(title,'(без названия)') FROM chats_seen ORDER BY last_seen DESC")
    rows = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
    con.close()
    return rows

@dp.callback_query(F.data == "menu:logs")
async def menu_logs(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.message.edit_text("❌ Нет доступа.", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    chats = get_seen_chats()
    if not chats:
        await cq.message.edit_text("Логов пока нет (бот ещё не видел чаты).", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    await cq.message.edit_text(
        "🧾 <b>Логи удалённой рекламы</b>\nВыбери чат:",
        reply_markup=kb_logs(chats)
    )
    await cq.answer()

def fetch_logs(chat_id: int, page: int) -> Tuple[List[tuple], bool, bool]:
    offset = page * LOGS_PAGE_SIZE
    con = db()
    cur = con.execute("""
        SELECT user_id, username, full_name, content, reason, keyword, created_at
        FROM deleted_ads_log
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (chat_id, LOGS_PAGE_SIZE + 1, offset))
    rows = cur.fetchall()
    con.close()
    has_next = len(rows) > LOGS_PAGE_SIZE
    rows = rows[:LOGS_PAGE_SIZE]
    has_prev = page > 0
    return rows, has_prev, has_next

@dp.callback_query(F.data.startswith("logs:chat:"))
async def logs_choose_chat(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    chat_id = int(cq.data.split(":")[-1])
    page = 0
    rows, has_prev, has_next = fetch_logs(chat_id, page)
    text = f"🧾 <b>Логи</b> (чат <code>{chat_id}</code>)\n\n"
    if not rows:
        text += "Пока пусто."
    else:
        for r in rows:
            uid, uname, fname, content, reason, kw, created_at = r
            dt = dt_to_str_local(int(created_at))
            who = f"@{uname}" if uname else fname
            text += (
                f"• <b>{html_escape(who)}</b> (<code>{uid}</code>)\n"
                f"  🕒 {dt}\n"
                f"  ⚠️ Причина: {html_escape(reason)} (ключ: <b>{html_escape(kw)}</b>)\n"
                f"  🧾 {html_escape(str(content)[:250])}\n\n"
            )
    await cq.message.edit_text(
        text,
        reply_markup=kb_logs_pager(chat_id, page, has_prev, has_next)
    )
    await cq.answer()

@dp.callback_query(F.data.startswith("logs:page:"))
async def logs_page(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, chat_id_s, page_s = cq.data.split(":")
    chat_id = int(chat_id_s)
    page = int(page_s)
    rows, has_prev, has_next = fetch_logs(chat_id, page)
    text = f"🧾 <b>Логи</b> (чат <code>{chat_id}</code>)\n\n"
    if not rows:
        text += "Пока пусто."
    else:
        for r in rows:
            uid, uname, fname, content, reason, kw, created_at = r
            dt = dt_to_str_local(int(created_at))
            who = f"@{uname}" if uname else fname
            text += (
                f"• <b>{html_escape(who)}</b> (<code>{uid}</code>)\n"
                f"  🕒 {dt}\n"
                f"  ⚠️ Причина: {html_escape(reason)} (ключ: <b>{html_escape(kw)}</b>)\n"
                f"  🧾 {html_escape(str(content)[:250])}\n\n"
            )
    await cq.message.edit_text(
        text,
        reply_markup=kb_logs_pager(chat_id, page, has_prev, has_next)
    )
    await cq.answer()


# =========================
# Поддержка: пользователь -> админы
# =========================
@dp.callback_query(F.data == "menu:support")
async def support_menu(cq: CallbackQuery):
    await cq.message.edit_text(
        "💬 <b>Связаться с администратором</b>\n\n"
        "Просто напиши сюда сообщение — я передам его админам.",
        reply_markup=kb_back("menu:main")
    )
    await cq.answer()

def support_save(user_id: int, username: str, full_name: str, text: str):
    con = db()
    con.execute("""
        INSERT INTO support_msgs(user_id,username,full_name,text,created_at,status)
        VALUES (?,?,?,?,?, 'open')
    """, (user_id, username, full_name, text, int(utcnow().timestamp())))
    con.commit()
    con.close()

def support_list(page: int) -> Tuple[List[tuple], bool, bool]:
    offset = page * LOGS_PAGE_SIZE
    con = db()
    cur = con.execute("""
        SELECT id, user_id, username, full_name, text, created_at
        FROM support_msgs
        WHERE status='open'
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (LOGS_PAGE_SIZE + 1, offset))
    rows = cur.fetchall()
    con.close()
    has_next = len(rows) > LOGS_PAGE_SIZE
    rows = rows[:LOGS_PAGE_SIZE]
    has_prev = page > 0
    return rows, has_prev, has_next

@dp.callback_query(F.data == "menu:inbox")
async def inbox_menu(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.message.edit_text("❌ Нет доступа.", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    await cq.message.edit_text(
        "📩 <b>Сообщения пользователей</b>\n\nОткрой список обращений:",
        reply_markup=kb_inbox(get_seen_chats())
    )
    await cq.answer()

@dp.callback_query(F.data.startswith("inbox:list:"))
async def inbox_list(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    page = int(cq.data.split(":")[-1])
    rows, has_prev, has_next = support_list(page)
    text = "📥 <b>Открытые обращения</b>\n\n"
    if not rows:
        text += "Пусто."
    else:
        for r in rows:
            sid, uid, uname, fname, txt, created_at = r
            dt = dt_to_str_local(int(created_at))
            who = f"@{uname}" if uname else fname
            text += f"• <b>{html_escape(who)}</b> (<code>{uid}</code>) | #{sid}\n  🕒 {dt}\n  🧾 {html_escape(str(txt)[:200])}\n\n"
        text += "Чтобы ответить: напиши команду в ЛС бота:\n<code>/reply #ID текст</code>\n"
    await cq.message.edit_text(text, reply_markup=kb_inbox_pager(page, has_prev, has_next))
    await cq.answer()

@dp.message(Command("reply"))
async def cmd_reply(msg: Message):
    if msg.chat.type != ChatType.PRIVATE:
        return
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split(maxsplit=2)
    if len(parts) < 3:
        return await msg.answer("Использование: /reply #123 текст ответа")
    sid_s = parts[1].lstrip("#")
    if not sid_s.isdigit():
        return await msg.answer("Нужно так: /reply #123 текст")
    sid = int(sid_s)
    reply_text = parts[2].strip()

    con = db()
    cur = con.execute("SELECT user_id FROM support_msgs WHERE id=? AND status='open'", (sid,))
    row = cur.fetchone()
    if not row:
        con.close()
        return await msg.answer("Не найдено открытое обращение с таким ID.")
    target_uid = int(row[0])

    con.execute("""
        UPDATE support_msgs
        SET status='closed', replied_by=?, reply_text=?, replied_at=?
        WHERE id=?
    """, (msg.from_user.id, reply_text, int(utcnow().timestamp()), sid))
    con.commit()
    con.close()

    # отправляем пользователю
    try:
        await bot.send_message(
            target_uid,
            f"✅ <b>Ответ администратора</b>\n\n{html_escape(reply_text)}"
        )
    except Exception:
        pass

    await msg.answer("✅ Ответ отправлен и обращение закрыто.")


# Если пользователь пишет в ЛС без команды — это сообщение в поддержку
@dp.message(F.chat.type == ChatType.PRIVATE, F.text)
async def private_text_router(msg: Message):
    txt = (msg.text or "").strip()
    if txt.startswith("/"):
        return
    # любое сообщение в ЛС от НЕ-админа = обращение в поддержку
    if not is_admin(msg.from_user.id):
        support_save(msg.from_user.id, msg.from_user.username or "", msg.from_user.full_name or "", txt)
        # уведомим админов
        for aid in ADMIN_IDS:
            try:
                await bot.send_message(
                    aid,
                    f"📩 <b>Новое обращение</b>\n"
                    f"От: {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)} "
                    f"(<code>{msg.from_user.id}</code>)\n\n"
                    f"{html_escape(txt)}"
                )
            except Exception:
                pass
        await msg.answer("✅ Сообщение отправлено администраторам.", reply_markup=kb_back("menu:main"))


# =========================
# Рассылка (только админы)
# =========================
@dp.callback_query(F.data == "menu:broadcast")
async def menu_broadcast(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.message.edit_text("❌ Нет доступа.", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    chats = get_seen_chats()
    if not chats:
        await cq.message.edit_text("Бот ещё не видел чаты для рассылки.", reply_markup=kb_back("menu:main"))
        return await cq.answer()
    await cq.message.edit_text(
        "📣 <b>Рассылка</b>\nВыбери чат:",
        reply_markup=kb_broadcast_choose_chat(chats)
    )
    await cq.answer()

# простой режим: админ выбирает чат → бот пишет "отправь текст" → админ отправляет /bc текст
BROADCAST_TARGET = {}  # admin_id -> chat_id

@dp.callback_query(F.data.startswith("bc:chat:"))
async def bc_choose_chat(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    chat_id = int(cq.data.split(":")[-1])
    BROADCAST_TARGET[cq.from_user.id] = chat_id
    await cq.message.edit_text(
        f"📣 Чат выбран: <code>{chat_id}</code>\n\n"
        "Теперь отправь команду:\n"
        "<code>/bc текст рассылки</code>\n\n"
        "Можно отправлять и с переносами строк.",
        reply_markup=kb_back("menu:broadcast")
    )
    await cq.answer()

@dp.message(Command("bc"))
async def cmd_bc(msg: Message):
    if msg.chat.type != ChatType.PRIVATE:
        return
    if not is_admin(msg.from_user.id):
        return
    chat_id = BROADCAST_TARGET.get(msg.from_user.id)
    if not chat_id:
        return await msg.answer("Сначала выбери чат в меню: Рассылка.")
    text = (msg.text or "").split(maxsplit=1)
    if len(text) < 2:
        return await msg.answer("Использование: /bc текст")
    payload = text[1]
    try:
        await bot.send_message(chat_id, payload)
        await msg.answer("✅ Отправлено.")
    except Exception as e:
        await msg.answer(f"❌ Ошибка отправки: {e}")


# =========================
# Команды: служебные
# =========================
@dp.message(Command("myid"))
async def cmd_myid(msg: Message):
    await msg.answer(f"🆔 Твой Telegram ID: <code>{msg.from_user.id}</code>")

@dp.message(Command("chatid"))
async def cmd_chatid(msg: Message):
    if msg.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        await msg.reply(f"chat_id этой группы: <code>{msg.chat.id}</code>")


# =========================
# Разрешения (команды): /adgive /adremove
# =========================
async def resolve_target_from_command_or_reply(msg: Message, arg: Optional[str]) -> Optional[int]:
    # 1) reply
    if msg.reply_to_message and msg.reply_to_message.from_user:
        # если указан @username в аргументе — используем аргумент, иначе reply
        if arg and arg.startswith("@"):
            # без API поиска не найдём ID по @, поэтому просим переслать или ID
            return None
        if arg and arg.isdigit():
            return int(arg)
        return msg.reply_to_message.from_user.id

    # 2) arg
    if not arg:
        return None
    if arg.isdigit():
        return int(arg)
    if arg.startswith("@"):
        # по @ без дополнительных методов ID не получить
        return None
    return None

@dp.message(Command("adgive"))
async def cmd_adgive(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    if msg.chat.type not in (ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP):
        return

    parts = (msg.text or "").split()
    # варианты:
    # /adgive @user 15m
    # /adgive 123 15m
    # reply: /adgive 15m
    arg1 = parts[1] if len(parts) >= 2 else None
    arg2 = parts[2] if len(parts) >= 3 else None

    # если reply и arg1 = время
    dur = None
    target_arg = arg1
    if msg.reply_to_message and arg1 and parse_duration(arg1) is not None and (arg2 is None):
        dur = parse_duration(arg1)
        target_arg = None
    else:
        dur = parse_duration(arg2)  # если есть 2-й аргумент как время

    target_id = await resolve_target_from_command_or_reply(msg, target_arg)
    if target_id is None:
        return await msg.reply(
            "❌ Не могу определить пользователя.\n\n"
            "Сделай так:\n"
            "1) <code>/adgive 123456789 1d</code>\n"
            "2) Ответом на сообщение: <code>/adgive 1d</code>\n"
            "3) Или пришли ID (по @ без API ID не получить)."
        )

    permit_set(msg.chat.id, target_id, dur)
    until = "Навсегда" if dur is None else dt_to_str_local(int((utcnow() + timedelta(seconds=dur)).timestamp()))
    await msg.reply(f"✅ Разрешение выдано: <code>{target_id}</code>\n⏳ До: <b>{until}</b>")

@dp.message(Command("adremove"))
async def cmd_adremove(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    if msg.chat.type not in (ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP):
        return

    parts = (msg.text or "").split()
    arg1 = parts[1] if len(parts) >= 2 else None
    target_id = await resolve_target_from_command_or_reply(msg, arg1)
    if target_id is None:
        return await msg.reply(
            "❌ Не могу определить пользователя.\n\n"
            "Сделай так:\n"
            "1) <code>/adremove 123456789</code>\n"
            "2) Ответом на сообщение: <code>/adremove</code>"
        )
    permit_remove(msg.chat.id, target_id)
    await msg.reply(f"🗑️ Разрешение убрано: <code>{target_id}</code>")


# =========================
# РУЧНЫЕ НАКАЗАНИЯ (mc*)
# =========================
def manual_save(chat_id: int, target: Message, ptype: str, issued_by: int, seconds: Optional[int], reason: str):
    exp = None
    if seconds is not None:
        exp = int((utcnow() + timedelta(seconds=seconds)).timestamp())
    con = db()
    con.execute("""
        INSERT INTO manual_punishments(chat_id,user_id,username,full_name,ptype,issued_by,issued_at,expires_at,reason)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        chat_id,
        target.from_user.id,
        target.from_user.username,
        target.from_user.full_name,
        ptype,
        issued_by,
        int(utcnow().timestamp()),
        exp,
        reason or "Причина не указана"
    ))
    con.commit()
    con.close()

def manual_warn_inc(chat_id: int, user_id: int) -> int:
    con = db()
    cur = con.execute("SELECT warns FROM manual_warn_counter WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    warns = int(row[0]) if row else 0
    warns += 1
    con.execute("INSERT OR REPLACE INTO manual_warn_counter(chat_id,user_id,warns) VALUES (?,?,?)", (chat_id, user_id, warns))
    con.commit()
    con.close()
    return warns

def manual_warn_clear(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM manual_warn_counter WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def parse_target_duration_reason(msg: Message) -> Tuple[Optional[int], Optional[int], str]:
    """
    Возвращает (target_id, duration_seconds, reason)
    Варианты:
    /mcban @user 1d причина
    /mcban 1d причина  (если reply)
    /mcban @user причина (навсегда)
    /mcban причина (если reply) (навсегда)
    """
    text = msg.text or ""
    parts = text.split(maxsplit=3)

    # reply target по умолчанию
    reply_target = msg.reply_to_message.from_user.id if msg.reply_to_message and msg.reply_to_message.from_user else None

    target_id = None
    duration = None
    reason = "Причина не указана"

    if len(parts) == 1:
        target_id = reply_target
        return target_id, None, reason

    # parts[1] может быть @user или время или id
    a1 = parts[1]
    a2 = parts[2] if len(parts) >= 3 else None
    a3 = parts[3] if len(parts) >= 4 else None

    # target by id
    if a1.isdigit():
        target_id = int(a1)
        # a2 duration?
        d = parse_duration(a2)
        if d is not None:
            duration = d
            reason = a3 or reason
        else:
            duration = None
            reason = " ".join(parts[2:]) if len(parts) >= 3 else reason
        return target_id, duration, reason

    # @username: без API ID не получить -> только если reply (или пусть пишут ID)
    if a1.startswith("@"):
        if reply_target is None:
            # нет reply — просим ID
            return None, None, "Нужно ID или ответом на сообщение."
        target_id = reply_target
        d = parse_duration(a2)
        if d is not None:
            duration = d
            reason = a3 or reason
        else:
            duration = None
            reason = " ".join(parts[2:]) if len(parts) >= 3 else reason
        return target_id, duration, reason

    # a1 как duration (если reply)
    d = parse_duration(a1)
    if d is not None:
        target_id = reply_target
        duration = d
        reason = " ".join(parts[2:]) if len(parts) >= 3 else reason
        return target_id, duration, reason

    # иначе это причина (если reply)
    target_id = reply_target
    duration = None
    reason = " ".join(parts[1:])
    return target_id, duration, reason


async def ensure_admin_and_group(msg: Message) -> bool:
    if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return False
    if not is_admin(msg.from_user.id):
        return False
    return True

async def apply_mute(chat_id: int, user_id: int, seconds: Optional[int]):
    until = None
    if seconds is not None:
        until = utcnow() + timedelta(seconds=seconds)
    await bot.restrict_chat_member(
        chat_id,
        user_id,
        permissions={"can_send_messages": False, "can_send_media_messages": False, "can_send_other_messages": False, "can_add_web_page_previews": False},
        until_date=until
    )

async def apply_unmute(chat_id: int, user_id: int):
    await bot.restrict_chat_member(
        chat_id,
        user_id,
        permissions={"can_send_messages": True, "can_send_media_messages": True, "can_send_other_messages": True, "can_add_web_page_previews": True}
    )

@dp.message(Command("mcwarn"))
async def cmd_mcwarn(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, _, reason = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или сделай ответом на сообщение.\nПример: <code>/mcwarn причина</code> (ответом)")
    # предупреждение счётчик 1/3 2/3 3/3 4/3 -> бан 3 дня
    warns = manual_warn_inc(msg.chat.id, target_id)

    # имя кликабельное
    tuser = msg.reply_to_message.from_user if msg.reply_to_message and msg.reply_to_message.from_user else msg.from_user
    mention = user_link_html(target_id, tuser.full_name, tuser.username)

    await msg.reply(
        f"⚠️ Предупреждение выдано: {mention}\n"
        f"📌 Причина: <b>{html_escape(reason)}</b>\n"
        f"📊 Счётчик: <b>{warns}/3</b>"
    )

    # если 4/3 -> бан 3 дня и сброс warn
    if warns >= 4:
        try:
            until = utcnow() + timedelta(days=3)
            await bot.ban_chat_member(msg.chat.id, target_id, until_date=until)
            manual_warn_clear(msg.chat.id, target_id)
            await msg.reply(f"⛔ Бан на 3 дня: {mention}\nПричина: превышение предупреждений (4/3).")
        except Exception as e:
            await msg.reply(f"❌ Не смог забанить: {e}")

@dp.message(Command("mcmute"))
async def cmd_mcmute(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, seconds, reason = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или сделай ответом на сообщение.\nПример: <code>/mcmute 1h причина</code> (ответом)")
    try:
        await apply_mute(msg.chat.id, target_id, seconds)
        await msg.reply(
            f"🔇 Мут: <code>{target_id}</code>\n"
            f"⏳ До: <b>{dt_to_str_local(int((utcnow()+timedelta(seconds=seconds)).timestamp())) if seconds else 'Навсегда'}</b>\n"
            f"📌 Причина: <b>{html_escape(reason)}</b>"
        )
    except Exception as e:
        await msg.reply(f"❌ Ошибка мута: {e}")

@dp.message(Command("mckick"))
async def cmd_mckick(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, _, reason = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или ответь на сообщение.")
    try:
        await bot.ban_chat_member(msg.chat.id, target_id)
        await bot.unban_chat_member(msg.chat.id, target_id)
        await msg.reply(f"👢 Кик: <code>{target_id}</code>\n📌 Причина: <b>{html_escape(reason)}</b>")
    except Exception as e:
        await msg.reply(f"❌ Ошибка кика: {e}")

@dp.message(Command("mcban"))
async def cmd_mcban(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, seconds, reason = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или ответь на сообщение.")
    try:
        until = None
        if seconds:
            until = utcnow() + timedelta(seconds=seconds)
        await bot.ban_chat_member(msg.chat.id, target_id, until_date=until)
        await msg.reply(
            f"⛔ Бан: <code>{target_id}</code>\n"
            f"⏳ До: <b>{dt_to_str_local(int(until.timestamp())) if until else 'Навсегда'}</b>\n"
            f"📌 Причина: <b>{html_escape(reason)}</b>"
        )
    except Exception as e:
        await msg.reply(f"❌ Ошибка бана: {e}")

@dp.message(Command("mcunwarn"))
async def cmd_mcunwarn(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, _, _ = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или ответь на сообщение.")
    manual_warn_clear(msg.chat.id, target_id)
    await msg.reply(f"✅ Предупреждения очищены: <code>{target_id}</code>")

@dp.message(Command("mcunmute"))
async def cmd_mcunmute(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, _, _ = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или ответь на сообщение.")
    try:
        await apply_unmute(msg.chat.id, target_id)
        await msg.reply(f"✅ Мут снят: <code>{target_id}</code>")
    except Exception as e:
        await msg.reply(f"❌ Ошибка снятия мута: {e}")

@dp.message(Command("mcunlock"))
async def cmd_mcunlock(msg: Message):
    # алиас
    await cmd_mcunmute(msg)

@dp.message(Command("mcunban"))
async def cmd_mcunban(msg: Message):
    if not await ensure_admin_and_group(msg):
        return
    target_id, _, _ = parse_target_duration_reason(msg)
    if not target_id:
        return await msg.reply("❌ Укажи ID или ответь на сообщение.")
    try:
        await bot.unban_chat_member(msg.chat.id, target_id)
        await msg.reply(f"✅ Бан снят: <code>{target_id}</code>")
    except Exception as e:
        await msg.reply(f"❌ Ошибка снятия бана: {e}")


@dp.message(Command("mclist"))
async def cmd_mclist(msg: Message):
    if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    # список доступен всем
    parts = (msg.text or "").split()
    page = 1
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))

    offset = (page - 1) * LIST_PAGE_SIZE
    con = db()
    cur = con.execute("""
        SELECT user_id, username, full_name, ptype, issued_at, expires_at, reason
        FROM manual_punishments
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (msg.chat.id, LIST_PAGE_SIZE + 1, offset))
    rows = cur.fetchall()
    con.close()

    has_next = len(rows) > LIST_PAGE_SIZE
    rows = rows[:LIST_PAGE_SIZE]

    text = f"📄 <b>Список наказаний</b> (стр. {page})\n\n"
    if not rows:
        text += "Пусто."
    else:
        for r in rows:
            uid, uname, fname, ptype, issued_at, exp, reason = r
            until = dt_to_str_local(exp) if exp else "Навсегда"
            active = "[Активно]" if (exp is None or exp == 0 or exp > int(utcnow().timestamp())) else "[Неактивно]"
            who = f"@{uname}" if uname else fname
            text += (
                f"• <b>{html_escape(who)}</b> (<code>{uid}</code>)\n"
                f"  Тип: <b>{ptype}</b> | До: <b>{until}</b> {active}\n"
                f"  Причина: {html_escape(reason)}\n\n"
            )
    if has_next:
        text += f"➡️ Следующая страница: <code>/mclist {page+1}</code>\n"
    if page > 1:
        text += f"⬅️ Предыдущая страница: <code>/mclist {page-1}</code>\n"
    await msg.reply(text)


# =========================
# АНТИ-РЕКЛАМА (текст + фото с caption)
# =========================
async def punish_ad(chat_id: int, msg: Message, keyword: str, has_perm: bool, has_tag: bool):
    # delete message
    try:
        await msg.delete()
    except Exception:
        pass

    # лог
    content = msg.text or msg.caption or ""
    log_deleted_ad(chat_id, msg, content, "Реклама", keyword)

    # если разрешения нет — стадийные наказания
    if not has_perm:
        # если написал #реклама без разрешения — отдельное сообщение
        if has_tag:
            await bot.send_message(
                chat_id,
                f"❌ {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)}\n"
                f"Ваше сообщение удалено: <b>нет разрешения на рекламу</b>.\n"
                f"Получить разрешение: {SUPPORT_BOT}\n"
                f"Правила: {RULES_LINK}"
            )
            return

        strikes = strikes_get(chat_id, msg.from_user.id) + 1
        if strikes == 1:
            strikes_set(chat_id, msg.from_user.id, strikes)
            await bot.send_message(
                chat_id,
                f"⚠️ {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)}\n"
                f"<b>Предупреждение</b> за рекламу.\n"
                f"Причина: ключевое слово: <b>{html_escape(keyword)}</b>\n"
                f"Ознакомиться с правилами: {RULES_LINK}\n"
                f"Получить разрешение: {SUPPORT_BOT}"
            )
        elif strikes == 2:
            strikes_set(chat_id, msg.from_user.id, strikes)
            try:
                await apply_mute(chat_id, msg.from_user.id, MUTE_STAGE_2)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                f"🔇 {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)}\n"
                f"Мут на <b>3 часа</b> за рекламу.\n"
                f"Причина: ключевое слово: <b>{html_escape(keyword)}</b>\n"
                f"Ознакомиться с правилами: {RULES_LINK}\n"
                f"Получить разрешение: {SUPPORT_BOT}"
            )
        else:
            # 3-я стадия: мут 12ч + сброс предупреждений бота
            try:
                await apply_mute(chat_id, msg.from_user.id, MUTE_STAGE_3)
            except Exception:
                pass
            strikes_reset(chat_id, msg.from_user.id)
            await bot.send_message(
                chat_id,
                f"🔇 {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)}\n"
                f"Мут на <b>12 часов</b> за рекламу.\n"
                f"Причина: ключевое слово: <b>{html_escape(keyword)}</b>\n"
                f"Ознакомиться с правилами: {RULES_LINK}\n"
                f"Получить разрешение: {SUPPORT_BOT}"
            )
        return

    # если разрешение есть, но нет тега в конце
    if has_perm and (not hashtag_at_end(content)):
        await bot.send_message(
            chat_id,
            f"🗑️ {user_link_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)}\n"
            f"Ваше сообщение удалено, по причине отсутствия тега на рекламу.\n"
            f"Пожалуйста укажите в вашей рекламе тег <b>\"{HASHTAG}\"</b> <b>в конце</b>."
        )
        return


@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def watcher(msg: Message):
    # чат запоминаем (для логов/рассылки)
    try:
        title = msg.chat.title or str(msg.chat.id)
        touch_chat(msg.chat.id, title)
    except Exception:
        pass

    # текст / caption
    content = msg.text or msg.caption or ""
    if not content:
        return

    # реклама?
    is_ad, keyword = contains_ad(content)
    if not is_ad:
        return

    uid = msg.from_user.id
    chat_id = msg.chat.id

    perm = permit_is_active(chat_id, uid)
    has_tag = has_hashtag_anywhere(content)

    # если есть разрешение, но рекламные сообщения можно раз в 24 часа
    if perm and is_ad and has_tag and hashtag_at_end(content):
        left = ad_cooldown_left(chat_id, uid)
        if left > 0:
            try:
                await msg.delete()
            except Exception:
                pass
            hrs = left // 3600
            mins = (left % 3600) // 60
            await bot.send_message(
                chat_id,
                f"⏳ {user_link_html(uid, msg.from_user.full_name, msg.from_user.username)}\n"
                f"Рекламу можно отправлять раз в <b>24 часа</b>.\n"
                f"Осталось: <b>{hrs}ч {mins}м</b>."
            )
            return
        ad_cooldown_mark(chat_id, uid)
        return  # всё ок, оставляем

    # если разрешения нет и есть #реклама — удалить и написать "нет разрешения"
    if (not perm) and has_tag:
        await punish_ad(chat_id, msg, keyword or "хэштег", has_perm=False, has_tag=True)
        return

    # если реклама и нет разрешения — стадийно
    if not perm:
        await punish_ad(chat_id, msg, keyword, has_perm=False, has_tag=False)
        return

    # если разрешение есть, но нет тега в конце — удалить и попросить
    if perm and (not hashtag_at_end(content)):
        await punish_ad(chat_id, msg, keyword, has_perm=True, has_tag=has_tag)
        return


# =========================
# STARTUP
# =========================
async def main():
    db().close()
    await bot.delete_webhook(drop_pending_updates=True)
    await setup_commands()
    print("[bot] starting polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
