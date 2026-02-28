# mc_ads_bot.py
# Aiogram 3.25.0
# - Анти-реклама (ссылки/телефон/ключевые слова)
# - Исключение: YouTube ссылки НЕ считаются рекламой
# - Ловит рекламу даже через РЕДАКТИРОВАНИЕ сообщения (edited_message)
# - Ловит IP-адреса и адреса Minecraft серверов (play.example.com / mc.example.net:25565)
# - Кнопки (callback_data) работают
# - Подсказки "/" в группах убраны (set_my_commands пусто для групп)
# - В ЛС есть меню с кнопками + админские кнопки

import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import logging
logging.basicConfig(level=logging.INFO)

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatPermissions,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


# =========================
# НАСТРОЙКИ
# =========================

def load_token() -> str:
    tok = os.environ.get("TOKEN")
    if tok:
        return tok.strip()

    # удобный fallback для Termux: положи токен в файл token.txt рядом со скриптом
    try:
        with open("token.txt", "r", encoding="utf-8") as f:
            t = f.read().strip()
            if t:
                return t
    except Exception:
        pass

    raise RuntimeError("TOKEN is not set. Set env TOKEN or create token.txt with token inside.")

TOKEN = load_token()

ADMIN_IDS = {8085895186}

DB_PATH = "mc_bot.db"
HASHTAG = "#реклама"

# анти-реклама: стадии наказаний (без разрешения)
MUTE_2_SECONDS = 3 * 60 * 60       # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60      # 12 часов

# лимит рекламы по разрешению
ADS_COOLDOWN_SECONDS = 24 * 60 * 60

# неактивность разрешения (если не юзал месяц)
PERMIT_INACTIVE_SECONDS = 30 * 24 * 60 * 60  # 30 дней

RULES_LINK = "https://leoned777.github.io/chats/"
SUPPORT_BOT_FOR_PERMIT = "@minecrfat_bot"

# стикер (опционально)
AD_WARN_STICKER_ID = None

# /mclist — по 10 записей
MC_LIST_PAGE_SIZE = 10


# =========================
# УТИЛИТЫ
# =========================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def ts() -> int:
    return int(now_utc().timestamp())

def fmt_dt(ts_int: int | None) -> str:
    if ts_int is None:
        return "Навсегда"
    dt = datetime.fromtimestamp(ts_int, tz=timezone.utc).astimezone()
    return dt.strftime("%d.%m.%Y %H:%M")

def fmt_duration_left(seconds_left: int) -> str:
    if seconds_left <= 0:
        return "0с"
    m, s = divmod(seconds_left, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    parts = []
    if d:
        parts.append(f"{d}д")
    if h:
        parts.append(f"{h}ч")
    if m:
        parts.append(f"{m}м")
    if not parts and s:
        parts.append(f"{s}с")
    return " ".join(parts)

def active_tag(until_ts: int | None, active_flag: int = 1) -> str:
    if active_flag == 0:
        return "[Неактивно]"
    if until_ts is None:
        return "[Активно]"
    return "[Активно]" if until_ts > ts() else "[Неактивно]"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def parse_duration(token: str | None) -> int | None:
    # 15m / 2h / 3d / 1w / 1y
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

def is_command_text(text: str | None) -> bool:
    return bool(text) and text.strip().startswith("/")

def hashtag_at_end(text: str) -> bool:
    return bool(re.search(r"#реклама\s*$", (text or "").lower()))

def has_hashtag(text: str) -> bool:
    return HASHTAG in (text or "").lower()

def mention_html(user_id: int, full_name: str) -> str:
    safe_name = (full_name or "Пользователь").replace("<", "").replace(">", "")
    return f'<a href="tg://user?id={user_id}">{safe_name}</a>'


# =========================
# АНТИ-РЕКЛАМА (детект)
# =========================

KW = ["продам", "куплю", "сдам", "прайс", "подпишитесь", "подписывайтесь"]

URL_RE = re.compile(r"(https?://[^\s]+|www\.[^\s]+)", re.I)
TME_RE = re.compile(r"(https?://)?t\.me/[\w_]{3,}", re.I)

PHONE_RE = re.compile(r"(\+?\d[\d\-\s]{8,}\d)")

# IPv4
IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")

# Домен (простая проверка) + опционально :порт
DOMAIN_PORT_RE = re.compile(
    r"\b([a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?)+)(?::(\d{2,5}))?\b",
    re.I
)

# Minecraft server обычно домен или IP, иногда с портом.
# Мы считаем рекламой ЛЮБОЙ IP или домен:порт (или домен с частыми MC поддоменами).
MC_HINT_RE = re.compile(r"\b(play|mc|mine|server|srv)\.", re.I)

YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "youtu.be", "www.youtu.be",
    "music.youtube.com",
}

def url_host(u: str) -> str | None:
    try:
        if u.lower().startswith("www."):
            u = "http://" + u
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if not host and p.path:
            # на случай кривых ссылок
            return None
        # убираем порт
        host = host.split("@")[-1].split(":")[0]
        return host or None
    except Exception:
        return None

def is_youtube_url(text: str) -> bool:
    for m in URL_RE.finditer(text or ""):
        host = url_host(m.group(0))
        if host and host in YOUTUBE_HOSTS:
            return True
    return False

def contains_mc_address(text: str) -> bool:
    t = (text or "").lower()

    # IP адреса
    if IPV4_RE.search(t):
        return True

    # домен (в т.ч. play., mc.) или домен:порт
    # если есть :порт — считаем точно как адрес сервера/реклама
    for m in DOMAIN_PORT_RE.finditer(t):
        domain = m.group(1)
        port = m.group(2)
        if port:
            return True
        if MC_HINT_RE.search(domain):
            return True

    return False

def is_ad_message(text: str | None) -> tuple[bool, str]:
    """
    Возвращает (True/False, причина)
    @username НЕ считаем рекламой.
    Исключение: YouTube ссылки разрешаем (не реклама).
    """
    t = (text or "").strip()
    low = t.lower()

    # YouTube не считаем рекламой
    if is_youtube_url(low):
        # но если в тексте есть ещё t.me / ip / домен:порт — это уже реклама
        if TME_RE.search(low) or IPV4_RE.search(low) or contains_mc_address(low):
            return True, "ссылка/адрес (кроме YouTube)"
        return False, "youtube"

    # t.me
    if TME_RE.search(low):
        return True, "ссылка t.me"

    # телефон
    if PHONE_RE.search(low):
        return True, "номер телефона"

    # ip/майнкрафт адрес
    if contains_mc_address(low):
        return True, "адрес сервера/IP"

    # ключевые слова
    for w in KW:
        if w in low:
            return True, f'ключевое слово: "{w}"'

    # обычные ссылки (кроме YouTube)
    if URL_RE.search(low):
        return True, "ссылка"

    return False, ""


# =========================
# БАЗА ДАННЫХ
# =========================
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
    CREATE TABLE IF NOT EXISTS permits (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        until_ts INTEGER,
        last_ad_ts INTEGER DEFAULT 0,
        PRIMARY KEY(chat_id, user_id)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS ad_strikes (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        stage INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(chat_id, user_id)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS cooldown_strikes (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(chat_id, user_id)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS deleted_ads_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        chat_title TEXT,
        user_id INTEGER NOT NULL,
        username TEXT,
        text_snip TEXT,
        reason TEXT,
        created_ts INTEGER NOT NULL
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS known_chats (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        updated_ts INTEGER NOT NULL
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS support_threads (
        user_id INTEGER PRIMARY KEY,
        last_ts INTEGER NOT NULL DEFAULT 0
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS mc_punishments (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        username TEXT,
        kind TEXT NOT NULL,             -- warn/mute/ban/kick
        until_ts INTEGER,               -- null=навсегда
        reason TEXT,
        issued_ts INTEGER NOT NULL,
        issued_by INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(chat_id, user_id, kind)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS admin_warns (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(chat_id, user_id)
    )""")
    con.commit()
    return con


# ----- чаты -----
def remember_chat(chat_id: int, title: str | None):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO known_chats(chat_id, title, updated_ts) VALUES (?,?,?)",
        (chat_id, title or "", ts())
    )
    con.commit()
    con.close()

def get_known_chats() -> list[tuple[int, str]]:
    con = db()
    rows = con.execute("SELECT chat_id, title FROM known_chats ORDER BY updated_ts DESC").fetchall()
    con.close()
    return [(int(r[0]), str(r[1] or "")) for r in rows]


# ----- разрешения -----
def permit_get(chat_id: int, user_id: int) -> tuple[bool, int | None, int]:
    con = db()
    row = con.execute(
        "SELECT until_ts, last_ad_ts FROM permits WHERE chat_id=? AND user_id=?",
        (chat_id, user_id)
    ).fetchone()
    con.close()
    if not row:
        return False, None, 0
    until_ts, last_ad_ts = row
    if until_ts is not None and int(until_ts) <= ts():
        return False, int(until_ts), int(last_ad_ts or 0)
    return True, (int(until_ts) if until_ts is not None else None), int(last_ad_ts or 0)

def permit_set(chat_id: int, user_id: int, until_ts: int | None):
    con = db()
    con.execute(
        """
        INSERT OR REPLACE INTO permits(chat_id, user_id, until_ts, last_ad_ts)
        VALUES (?,?,?, COALESCE((SELECT last_ad_ts FROM permits WHERE chat_id=? AND user_id=?), 0))
        """,
        (chat_id, user_id, until_ts, chat_id, user_id)
    )
    con.commit()
    con.close()

def permit_remove(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

def permit_touch_last_ad(chat_id: int, user_id: int):
    con = db()
    con.execute("UPDATE permits SET last_ad_ts=? WHERE chat_id=? AND user_id=?", (ts(), chat_id, user_id))
    con.commit()
    con.close()

def permits_list_active(chat_id: int) -> list[tuple[int, int | None, int]]:
    now = ts()
    con = db()
    rows = con.execute(
        """
        SELECT user_id, until_ts, last_ad_ts
        FROM permits
        WHERE chat_id=?
          AND (until_ts IS NULL OR until_ts > ?)
        ORDER BY last_ad_ts DESC
        """,
        (chat_id, now)
    ).fetchall()
    con.close()
    out = []
    for r in rows:
        out.append((int(r[0]), (int(r[1]) if r[1] is not None else None), int(r[2] or 0)))
    return out


# ----- стадии рекламы (без разрешения) -----
def ad_stage_get(chat_id: int, user_id: int) -> int:
    con = db()
    row = con.execute("SELECT stage FROM ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else 0

def ad_stage_set(chat_id: int, user_id: int, stage: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO ad_strikes(chat_id, user_id, stage) VALUES (?,?,?)", (chat_id, user_id, stage))
    con.commit()
    con.close()


# ----- cooldown предупреждения (если разрешение есть, но раньше 24ч) -----
def cooldown_warn_get(chat_id: int, user_id: int) -> int:
    con = db()
    row = con.execute("SELECT count FROM cooldown_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else 0

def cooldown_warn_set(chat_id: int, user_id: int, count: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO cooldown_strikes(chat_id, user_id, count) VALUES (?,?,?)", (chat_id, user_id, count))
    con.commit()
    con.close()

def cooldown_warn_reset(chat_id: int, user_id: int):
    cooldown_warn_set(chat_id, user_id, 0)


# ----- логи рекламы -----
def log_deleted_ad(chat_id: int, chat_title: str, user_id: int, username: str | None, text: str, reason: str):
    snip = (text or "").strip().replace("\n", " ")
    snip = snip[:280]
    con = db()
    con.execute(
        "INSERT INTO deleted_ads_log(chat_id, chat_title, user_id, username, text_snip, reason, created_ts) VALUES (?,?,?,?,?,?,?)",
        (chat_id, chat_title or "", user_id, username or "", snip, reason, ts())
    )
    con.commit()
    con.close()


# ----- support -----
def support_touch_user(uid: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO support_threads(user_id, last_ts) VALUES (?,?)", (uid, ts()))
    con.commit()
    con.close()

def support_users_list() -> list[int]:
    con = db()
    rows = con.execute("SELECT user_id FROM support_threads ORDER BY last_ts DESC").fetchall()
    con.close()
    return [int(r[0]) for r in rows]


# ----- наказания (для /mclist) -----
def mc_upsert(chat_id: int, user_id: int, username: str | None, kind: str, until_ts: int | None, reason: str, issued_by: int, active: int):
    con = db()
    con.execute(
        """
        INSERT OR REPLACE INTO mc_punishments(chat_id,user_id,username,kind,until_ts,reason,issued_ts,issued_by,active)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (chat_id, user_id, username or "", kind, until_ts, reason, ts(), issued_by, active)
    )
    con.commit()
    con.close()

def mc_list(chat_id: int, page: int) -> tuple[list[tuple], int]:
    con = db()
    total = con.execute("SELECT COUNT(*) FROM mc_punishments WHERE chat_id=?", (chat_id,)).fetchone()[0]
    offset = (page - 1) * MC_LIST_PAGE_SIZE
    rows = con.execute(
        """
        SELECT user_id, username, kind, until_ts, reason, issued_ts, active
        FROM mc_punishments
        WHERE chat_id=?
        ORDER BY issued_ts DESC
        LIMIT ? OFFSET ?
        """,
        (chat_id, MC_LIST_PAGE_SIZE, offset)
    ).fetchall()
    con.close()
    return rows, int(total)


# ----- админ-варны (счётчик) -----
def admin_warn_get(chat_id: int, user_id: int) -> int:
    con = db()
    row = con.execute("SELECT count FROM admin_warns WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else 0

def admin_warn_set(chat_id: int, user_id: int, count: int):
    con = db()
    con.execute("INSERT OR REPLACE INTO admin_warns(chat_id, user_id, count) VALUES (?,?,?)", (chat_id, user_id, count))
    con.commit()
    con.close()


# =========================
# FSM (ЛС)
# =========================
class AdminStates(StatesGroup):
    waiting_permit_give = State()
    waiting_permit_remove = State()
    waiting_broadcast_chat = State()
    waiting_broadcast_message = State()
    waiting_support_reply_pick = State()
    waiting_support_reply_text = State()


# =========================
# КЛАВИАТУРЫ (ЛС)
# =========================
def kb_main(is_admin_flag: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🆔 Узнать ID", callback_data="my_id")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
    ]
    if is_admin_flag:
        rows += [
            [InlineKeyboardButton(text="✅ Разрешения (выдать/забрать)", callback_data="perm_menu")],
            [InlineKeyboardButton(text="📋 Список разрешений", callback_data="perm_list_pick_chat")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="bc_menu")],
            [InlineKeyboardButton(text="💬 Сообщения", callback_data="support_admin")],
        ]
    rows += [
        [InlineKeyboardButton(text="☎️ Связь с админом", callback_data="support_user")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(to: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=to)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")],
    ])

def kb_perm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать разрешение", callback_data="perm_give")],
        [InlineKeyboardButton(text="➖ Забрать разрешение", callback_data="perm_remove")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")],
    ])

def kb_bc_chats(chats: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chats[:25]:
        label = title if title else str(cid)
        rows.append([InlineKeyboardButton(text=f"📣 {label[:40]}", callback_data=f"bc_chat:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_support_admin_users(users: list[int]) -> InlineKeyboardMarkup:
    rows = []
    for uid in users[:25]:
        rows.append([InlineKeyboardButton(text=f"👤 {uid}", callback_data=f"sup_user:{uid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_mclist(chat_id: int, page: int, total: int) -> InlineKeyboardMarkup:
    max_page = max(1, (total + MC_LIST_PAGE_SIZE - 1) // MC_LIST_PAGE_SIZE)
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"mclist:{chat_id}:{page-1}"))
    if page < max_page:
        buttons.append(InlineKeyboardButton(text="➡️ Дальше", callback_data=f"mclist:{chat_id}:{page+1}"))
    rows = [buttons] if buttons else []
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_perm_list_pick_chat(chats: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, title in chats[:25]:
        label = title if title else str(cid)
        rows.append([InlineKeyboardButton(text=f"🗂 {label[:40]}", callback_data=f"perm_list:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_regrant(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Выдать", callback_data=f"regrant:{chat_id}:{user_id}"),
            InlineKeyboardButton(text="❌ Не выдавать", callback_data="noop")
        ]
    ])


# =========================
# БОТ
# =========================
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


# =========================
# УДАЛЕНИЕ СООБЩЕНИЙ
# =========================
async def try_delete(msg: Message) -> bool:
    try:
        await msg.delete()
        return True
    except Exception:
        return False

async def ensure_delete_warning(chat_id: int):
    await bot.send_message(
        chat_id,
        "⚠️ Я не смог удалить сообщение.\n"
        "Дай мне права: <b>Delete messages</b> (сделай админом)."
    )

async def notify_admins(text: str, kb: InlineKeyboardMarkup | None = None):
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, text, reply_markup=kb)
        except Exception:
            pass


# =========================
# ГРУППА: наказания (mute/ban)
# =========================
async def apply_mute(chat_id: int, user_id: int, seconds: int | None):
    if seconds is None:
        perms = ChatPermissions(can_send_messages=False)
        await bot.restrict_chat_member(chat_id, user_id, permissions=perms)
        return
    until = now_utc() + timedelta(seconds=seconds)
    perms = ChatPermissions(can_send_messages=False)
    await bot.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)

async def apply_unmute(chat_id: int, user_id: int):
    perms = ChatPermissions(
        can_send_messages=True, can_send_audios=True, can_send_documents=True,
        can_send_photos=True, can_send_videos=True, can_send_video_notes=True,
        can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True,
        can_add_web_page_previews=True, can_invite_users=True,
        can_change_info=False, can_pin_messages=False
    )
    await bot.restrict_chat_member(chat_id, user_id, permissions=perms)

async def apply_ban(chat_id: int, user_id: int, seconds: int | None):
    until = None
    if seconds is not None:
        until = now_utc() + timedelta(seconds=seconds)
    await bot.ban_chat_member(chat_id, user_id, until_date=until)

async def apply_unban(chat_id: int, user_id: int):
    await bot.unban_chat_member(chat_id, user_id)


# =========================
# РАЗБОР ЦЕЛИ ДЛЯ КОМАНД
# =========================
async def resolve_target_from_command(msg: Message, args: list[str]) -> int | None:
    # 1) reply — если нет @/id в аргументах
    if msg.reply_to_message and msg.reply_to_message.from_user:
        if not args:
            return msg.reply_to_message.from_user.id

    # 2) аргументы: id или @username
    if args:
        t = args[0].strip()
        if t.isdigit():
            return int(t)
        if t.startswith("@"):
            username = t[1:]
            try:
                ch = await bot.get_chat(username)
                return int(ch.id)
            except Exception:
                return None
    return None

def split_args(text: str | None) -> list[str]:
    if not text:
        return []
    parts = text.strip().split()
    return parts[1:] if len(parts) > 1 else []


# =========================
# /start /cancel /chatid /userid
# =========================
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    await state.clear()
    await msg.answer(
        "🏠 <b>Главное меню</b>\n\n"
        f"📌 Тег рекламы в конце: <code>{HASHTAG}</code>\n"
        f"📖 Правила: {RULES_LINK}",
        reply_markup=kb_main(is_admin(msg.from_user.id))
    )

@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    await state.clear()
    await msg.answer("❌ Отменено.", reply_markup=kb_main(is_admin(msg.from_user.id)))

@dp.message(Command("chatid"))
async def cmd_chatid(msg: Message):
    if msg.chat.type in ("group", "supergroup"):
        await msg.reply(f"✅ chat_id: <code>{msg.chat.id}</code>")
    else:
        await msg.answer("ℹ️ <b>/chatid</b> работает только в группе.")

@dp.message(Command("userid"))
async def cmd_userid(msg: Message):
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        await msg.reply(f"🆔 ID пользователя: <code>{u.id}</code>")
        return
    if msg.forward_from:
        await msg.reply(f"🆔 ID пользователя: <code>{msg.forward_from.id}</code>")
        return
    await msg.reply(f"🆔 Твой ID: <code>{msg.from_user.id}</code>")


# =========================
# ГРУППА: /adgive /adrevoke (нужные админские)
# =========================
@dp.message(Command("adgive"))
async def cmd_adgive(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    uid = await resolve_target_from_command(msg, args)
    if uid is None:
        await msg.reply("ℹ️ Формат: <code>/adgive @user 1d</code> или ответом: <code>/adgive 1d</code>")
        return

    rest = args[1:] if args and (args[0].startswith("@") or args[0].isdigit()) else args
    dur = parse_duration(rest[0]) if rest else None
    until_ts = None if dur is None else ts() + dur
    permit_set(msg.chat.id, uid, until_ts)
    cooldown_warn_reset(msg.chat.id, uid)

    await msg.reply(f"✅ Разрешение на рекламу выдано: <code>{uid}</code>\n⏳ До: <b>{fmt_dt(until_ts)}</b>")

@dp.message(Command("adrevoke"))
async def cmd_adrevoke(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    uid = await resolve_target_from_command(msg, args)
    if uid is None:
        await msg.reply("ℹ️ Формат: <code>/adrevoke @user</code> или ответом: <code>/adrevoke</code>")
        return

    permit_remove(msg.chat.id, uid)
    cooldown_warn_reset(msg.chat.id, uid)
    await msg.reply(f"🗑️ Разрешение на рекламу убрано: <code>{uid}</code>")


# =========================
# ГРУППА: снятие наказаний
# =========================
@dp.message(Command("mcunwarn"))
async def cmd_mcunwarn(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    uid = await resolve_target_from_command(msg, args)
    if uid is None:
        await msg.reply("ℹ️ Формат: <code>/mcunwarn @user</code> или ответом: <code>/mcunwarn</code>")
        return

    admin_warn_set(msg.chat.id, uid, 0)
    mc_upsert(msg.chat.id, uid, "", "warn", ts(), "Снято админом", msg.from_user.id, 0)
    await msg.reply(f"✅ Предупреждения сняты: <code>{uid}</code>")

@dp.message(Command("mcunmute"))
async def cmd_mcunmute(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    uid = await resolve_target_from_command(msg, args)
    if uid is None:
        await msg.reply("ℹ️ Формат: <code>/mcunmute @user</code> или ответом: <code>/mcunmute</code>")
        return

    try:
        await apply_unmute(msg.chat.id, uid)
    except Exception:
        pass
    mc_upsert(msg.chat.id, uid, "", "mute", ts(), "Снято админом", msg.from_user.id, 0)
    await msg.reply(f"✅ Мут снят: <code>{uid}</code>")

@dp.message(Command("mcunban"))
async def cmd_mcunban(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    uid = await resolve_target_from_command(msg, args)
    if uid is None:
        await msg.reply("ℹ️ Формат: <code>/mcunban @user</code> или ответом: <code>/mcunban</code>")
        return

    try:
        await apply_unban(msg.chat.id, uid)
    except Exception:
        pass
    mc_upsert(msg.chat.id, uid, "", "ban", ts(), "Снято админом", msg.from_user.id, 0)
    await msg.reply(f"✅ Бан снят: <code>{uid}</code>")


# =========================
# /mclist
# =========================
def kind_ru(kind: str) -> str:
    return {
        "warn": "WARN",
        "mute": "MUTE",
        "ban": "BAN",
        "kick": "KICK",
    }.get(kind, kind.upper())

def render_mclist(chat_id: int, page: int) -> tuple[str, InlineKeyboardMarkup]:
    rows, total = mc_list(chat_id, page)
    if not rows:
        return "📋 <b>Список наказаний пуст.</b>", InlineKeyboardMarkup(inline_keyboard=[])

    lines = [f"📋 <b>Список наказаний</b> (стр. {page})", ""]
    for (user_id, username, kind, until_ts, reason, issued_ts, active) in rows:
        user_link = f'<a href="tg://user?id={user_id}">{username or user_id}</a>'
        until_str = f"{fmt_dt(until_ts)} {active_tag(until_ts, active)}"
        reason = reason or "Причина не указана"
        lines.append(
            f"• {user_link} — <b>{kind_ru(kind)}</b>\n"
            f"  ⏳ {until_str}\n"
            f"  📝 {reason}"
        )

    kb = kb_mclist(chat_id, page, total)
    return "\n".join(lines), kb

@dp.message(Command("mclist"))
async def cmd_mclist(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    args = split_args(msg.text)
    page = 1
    if args and args[0].isdigit():
        page = max(1, int(args[0]))

    text, kb = render_mclist(msg.chat.id, page)
    await msg.reply(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("mclist:"))
async def cb_mclist(cq: CallbackQuery):
    try:
        _, chat_id_s, page_s = cq.data.split(":")
        chat_id = int(chat_id_s)
        page = int(page_s)
    except Exception:
        await cq.answer()
        return

    if cq.message.chat.id != chat_id:
        await cq.answer()
        return

    text, kb = render_mclist(chat_id, page)
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass
    await cq.answer()


# =========================
# CALLBACK: меню (ЛС) — КНОПКИ
# =========================
@dp.callback_query(F.data == "menu")
async def cb_menu(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.edit_text("🏠 <b>Главное меню</b>", reply_markup=kb_main(is_admin(cq.from_user.id)))
    await cq.answer()

@dp.callback_query(F.data == "cancel")
async def cb_cancel(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.edit_text("❌ Отменено.", reply_markup=kb_main(is_admin(cq.from_user.id)))
    await cq.answer()

@dp.callback_query(F.data == "my_id")
async def cb_myid(cq: CallbackQuery):
    await cq.message.edit_text(
        f"🆔 <b>Твой Telegram ID:</b> <code>{cq.from_user.id}</code>",
        reply_markup=kb_back("menu")
    )
    await cq.answer()

@dp.callback_query(F.data == "profile")
async def cb_profile(cq: CallbackQuery):
    role = "Админ" if is_admin(cq.from_user.id) else "Участник"
    await cq.message.edit_text(
        "👤 <b>Профиль</b>\n\n"
        f"⭐ Статус: <b>{role}</b>\n"
        f"🆔 ID: <code>{cq.from_user.id}</code>\n",
        reply_markup=kb_back("menu")
    )
    await cq.answer()


# =========================
# ЛС: Разрешения / Список / Рассылка / Support
# =========================
@dp.callback_query(F.data == "perm_menu")
async def cb_perm_menu(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await cq.message.edit_text(
        "✅ <b>Разрешения на рекламу</b>\n\n"
        "После выбора действия <b>не нужны команды</b> — просто пришли:\n"
        "• <code>@username</code>\n"
        "• или <code>ID</code>\n"
        "• или <b>перешли сообщение</b> пользователя\n\n"
        "Можно указать срок: <code>@user 15m</code>\n"
        "Если срок не указать — навсегда.",
        reply_markup=kb_perm()
    )
    await cq.answer()

@dp.callback_query(F.data == "perm_list_pick_chat")
async def cb_perm_list_pick_chat(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    chats = get_known_chats()
    if not chats:
        await cq.message.edit_text(
            "📋 <b>Список разрешений</b>\n\n"
            "Пока нет чатов.\n"
            "Напиши что-нибудь в группе с ботом — и чат появится.",
            reply_markup=kb_back("menu")
        )
        await cq.answer()
        return
    await cq.message.edit_text("📋 <b>Выбери чат</b>:", reply_markup=kb_perm_list_pick_chat(chats))
    await cq.answer()

@dp.callback_query(F.data.startswith("perm_list:"))
async def cb_perm_list(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    chat_id = int(cq.data.split(":")[1])
    items = permits_list_active(chat_id)

    if not items:
        await cq.message.edit_text(
            "📋 <b>Разрешений нет</b>\n\n"
            "В этом чате пока никто не имеет разрешения.",
            reply_markup=kb_back("perm_list_pick_chat")
        )
        await cq.answer()
        return

    lines = ["📋 <b>Разрешения на рекламу</b>", ""]
    now = ts()
    for user_id, until_ts, last_ad_ts in items[:50]:
        inactive = (last_ad_ts == 0) or (now - last_ad_ts > PERMIT_INACTIVE_SECONDS)
        status = "[Неактивно]" if inactive else "[Активно]"
        last_used = "никогда" if last_ad_ts == 0 else fmt_dt(last_ad_ts)
        user_link = f'<a href="tg://user?id={user_id}">{user_id}</a>'
        lines.append(
            f"• {user_link} — <b>{status}</b>\n"
            f"  ⏳ До: <b>{fmt_dt(until_ts)}</b>\n"
            f"  🕒 Последняя реклама: <b>{last_used}</b>"
        )

    await cq.message.edit_text("\n".join(lines), reply_markup=kb_back("perm_list_pick_chat"))
    await cq.answer()

@dp.callback_query(F.data == "perm_give")
async def cb_perm_give(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_permit_give)
    await cq.message.edit_text(
        "➕ <b>Выдать разрешение</b>\n\n"
        "Пришли: <code>@username</code> / <code>ID</code> / пересланное сообщение.\n"
        "Срок можно добавить: <code>@user 1d</code>\n"
        "Если срок не указать — навсегда.",
        reply_markup=kb_back("perm_menu")
    )
    await cq.answer()

@dp.callback_query(F.data == "perm_remove")
async def cb_perm_remove(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_permit_remove)
    await cq.message.edit_text(
        "➖ <b>Забрать разрешение</b>\n\n"
        "Пришли: <code>@username</code> / <code>ID</code> / пересланное сообщение.",
        reply_markup=kb_back("perm_menu")
    )
    await cq.answer()

async def resolve_user_id_from_input(msg: Message, raw: str | None) -> int | None:
    # 1) forward (если не скрыт)
    if msg.forward_from:
        return msg.forward_from.id
    # 2) id числом
    if raw and raw.strip().isdigit():
        return int(raw.strip())
    # 3) @username -> get_chat
    if raw:
        t = raw.strip()
        if t.startswith("@"):
            t = t[1:]
            try:
                ch = await bot.get_chat(t)
                return int(ch.id)
            except Exception:
                return None
    return None

@dp.message(AdminStates.waiting_permit_give)
async def st_perm_give(msg: Message, state: FSMContext):
    if msg.chat.type != "private" or not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").strip().split()
    raw_target = parts[0] if parts else None
    raw_dur = parts[1] if len(parts) >= 2 else None

    uid = await resolve_user_id_from_input(msg, raw_target)
    if uid is None:
        await msg.answer("❌ Не смог определить ID. Пришли ID / @username / пересланное сообщение.")
        return

    dur_sec = parse_duration(raw_dur)
    until_ts = None if dur_sec is None else ts() + dur_sec

    chats = get_known_chats()
    if not chats:
        await msg.answer("⚠️ Я ещё не знаю чаты. Напиши что-нибудь в группе с ботом и повтори.")
        return

    for chat_id, _ in chats:
        permit_set(chat_id, uid, until_ts)
        cooldown_warn_reset(chat_id, uid)

    await state.clear()
    await msg.answer(
        "✅ <b>Разрешение выдано</b>\n\n"
        f"🆔 <code>{uid}</code>\n"
        f"⏳ До: <b>{fmt_dt(until_ts)}</b>",
        reply_markup=kb_main(True)
    )

@dp.message(AdminStates.waiting_permit_remove)
async def st_perm_remove(msg: Message, state: FSMContext):
    if msg.chat.type != "private" or not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").strip().split()
    raw_target = parts[0] if parts else None

    uid = await resolve_user_id_from_input(msg, raw_target)
    if uid is None:
        await msg.answer("❌ Не смог определить ID. Пришли ID / @username / пересланное сообщение.")
        return

    chats = get_known_chats()
    for chat_id, _ in chats:
        permit_remove(chat_id, uid)
        cooldown_warn_reset(chat_id, uid)

    await state.clear()
    await msg.answer(
        "🗑️ <b>Разрешение убрано</b>\n\n"
        f"🆔 <code>{uid}</code>",
        reply_markup=kb_main(True)
    )


# =========================
# ЛС: Рассылка
# =========================
@dp.callback_query(F.data == "bc_menu")
async def cb_bc_menu(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return

    chats = get_known_chats()
    if not chats:
        await cq.message.edit_text(
            "📣 <b>Рассылка</b>\n\n"
            "Пока нет чатов в списке.\n"
            "Напиши что-нибудь в группе с ботом — и чат появится.",
            reply_markup=kb_back("menu")
        )
        await cq.answer()
        return

    await state.set_state(AdminStates.waiting_broadcast_chat)
    await cq.message.edit_text("📣 <b>Выбери чат</b>:", reply_markup=kb_bc_chats(chats))
    await cq.answer()

@dp.callback_query(F.data.startswith("bc_chat:"))
async def cb_bc_chat(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return

    chat_id = int(cq.data.split(":")[1])
    await state.update_data(bc_chat_id=chat_id)
    await state.set_state(AdminStates.waiting_broadcast_message)
    await cq.message.edit_text(
        "✉️ <b>Отправь сообщение для рассылки</b>\n\n"
        "Текст/фото/видео/док — всё можно.",
        reply_markup=kb_back("menu")
    )
    await cq.answer()

@dp.message(AdminStates.waiting_broadcast_message)
async def st_bc_send(msg: Message, state: FSMContext):
    if msg.chat.type != "private" or not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    chat_id = data.get("bc_chat_id")

    if not chat_id:
        await state.clear()
        await msg.answer("⚠️ Сначала выбери чат.", reply_markup=kb_main(True))
        return

    try:
        await bot.copy_message(chat_id=chat_id, from_chat_id=msg.chat.id, message_id=msg.message_id)
        await msg.answer("✅ Отправлено.", reply_markup=kb_main(True))
    except Exception as e:
        await msg.answer(f"❌ Ошибка отправки: <code>{type(e).__name__}</code>", reply_markup=kb_main(True))
    finally:
        await state.clear()


# =========================
# ЛС: Support
# =========================
@dp.callback_query(F.data == "support_user")
async def cb_support_user(cq: CallbackQuery):
    await cq.message.edit_text(
        "☎️ <b>Связь с админом</b>\n\n"
        "Напиши сюда сообщение — я перешлю админу.",
        reply_markup=kb_back("menu")
    )
    await cq.answer()

@dp.callback_query(F.data == "support_admin")
async def cb_support_admin(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return

    users = support_users_list()
    if not users:
        await cq.message.edit_text("💬 Сообщений пока нет.", reply_markup=kb_back("menu"))
        await cq.answer()
        return

    await state.set_state(AdminStates.waiting_support_reply_pick)
    await cq.message.edit_text("💬 <b>Выбери пользователя</b>:", reply_markup=kb_support_admin_users(users))
    await cq.answer()

@dp.callback_query(F.data.startswith("sup_user:"))
async def cb_sup_user_pick(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return

    uid = int(cq.data.split(":")[1])
    await state.update_data(support_uid=uid)
    await state.set_state(AdminStates.waiting_support_reply_text)
    await cq.message.edit_text(
        f"✍️ Напиши ответ пользователю <code>{uid}</code>:",
        reply_markup=kb_back("support_admin")
    )
    await cq.answer()

@dp.message(AdminStates.waiting_support_reply_text)
async def st_sup_reply(msg: Message, state: FSMContext):
    if msg.chat.type != "private" or not is_admin(msg.from_user.id):
        return

    data = await state.get_data()
    uid = data.get("support_uid")

    if not uid:
        await state.clear()
        await msg.answer("⚠️ Сначала выбери пользователя.", reply_markup=kb_main(True))
        return

    try:
        await bot.send_message(uid, f"💬 <b>Ответ администратора:</b>\n\n{msg.text or ''}")
        await msg.answer("✅ Отправлено.", reply_markup=kb_main(True))
    finally:
        await state.clear()


# =========================
# PRIVATE CATCHALL (ЛС)
# =========================
@dp.message(F.chat.type == "private")
async def private_catchall(msg: Message):
    # неизвестные команды в ЛС
    if msg.text and msg.text.startswith("/"):
        allow = {"/start", "/cancel", "/chatid", "/userid"}
        if msg.text.split()[0] not in allow:
            await msg.answer("ℹ️ Нажми /start чтобы открыть меню.")
            return

    support_touch_user(msg.from_user.id)
    for aid in ADMIN_IDS:
        try:
            uname = f"@{msg.from_user.username}" if msg.from_user.username else ""
            await bot.send_message(
                aid,
                f"📩 <b>Сообщение от пользователя</b>\n"
                f"🆔 <code>{msg.from_user.id}</code> {uname}\n\n"
                f"{msg.text or ''}"
            )
        except Exception:
            pass

    await msg.answer("✅ Сообщение отправлено админу.", reply_markup=kb_main(is_admin(msg.from_user.id)))


# =========================
# CALLBACK: regrant
# =========================
@dp.callback_query(F.data.startswith("regrant:"))
async def cb_regrant(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    try:
        _, chat_id_s, user_id_s = cq.data.split(":")
        chat_id = int(chat_id_s)
        user_id = int(user_id_s)
    except Exception:
        await cq.answer()
        return

    permit_set(chat_id, user_id, None)
    cooldown_warn_reset(chat_id, user_id)

    try:
        await cq.message.edit_text(cq.message.html_text + "\n\n✅ <b>Разрешение снова выдано.</b>")
    except Exception:
        pass

    try:
        await bot.send_message(chat_id, f"✅ Пользователю <code>{user_id}</code> снова выдано разрешение на рекламу.")
    except Exception:
        pass

    await cq.answer("Выдано ✅")

@dp.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery):
    await cq.answer("Ок")


# =========================
# АНТИ-РЕКЛАМА: общая логика (для msg и edited_message)
# =========================
async def handle_ad_check(msg: Message, edited: bool = False):
    remember_chat(msg.chat.id, msg.chat.title)

    # команды не трогаем
    if is_command_text(msg.text) or is_command_text(msg.caption):
        return

    text = msg.text or msg.caption or ""
    if not text:
        return

    ad, reason_detail = is_ad_message(text)

    # если не реклама и нет хэштега — игнор
    if (not ad) and (not has_hashtag(text)):
        return

    chat_id = msg.chat.id
    uid = msg.from_user.id
    chat_title = msg.chat.title or ""
    user_mention = mention_html(uid, msg.from_user.full_name)

    permit_ok, _permit_until, last_ad_ts = permit_get(chat_id, uid)
    edit_tag = " (редактирование)" if edited else ""

    # (1) без разрешения, но пишет #реклама
    if (not permit_ok) and has_hashtag(text):
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)

        await bot.send_message(chat_id, f"❌ У вас нет разрешения на рекламу{edit_tag}.\nПолучить: {SUPPORT_BOT_FOR_PERMIT}")
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"нет разрешения, но есть #реклама{edit_tag}")
        return

    # (2) есть разрешение, но реклама без тега в конце
    if permit_ok and ad and (not hashtag_at_end(text)):
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)

        await bot.send_message(
            chat_id,
            f"{user_mention}, 🗑️ ваше сообщение удалено{edit_tag}.\n"
            f"Причина: <b>нет тега {HASHTAG} в конце</b>\n"
            f"Правила: {RULES_LINK}"
        )
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"разрешение есть, но тег не в конце ({reason_detail}){edit_tag}")
        return

    # (3) есть разрешение и реклама — лимит 24ч
    if permit_ok and ad:
        if last_ad_ts and (ts() - last_ad_ts) < ADS_COOLDOWN_SECONDS:
            deleted = await try_delete(msg)
            if not deleted:
                await ensure_delete_warning(chat_id)

            left = ADS_COOLDOWN_SECONDS - (ts() - last_ad_ts)
            warn_count = cooldown_warn_get(chat_id, uid) + 1
            cooldown_warn_set(chat_id, uid, warn_count)

            await bot.send_message(
                chat_id,
                f"⏳ {user_mention}, реклама раз в <b>24 часа</b>{edit_tag}.\n"
                f"Осталось ждать: <b>{fmt_duration_left(left)}</b>\n"
                f"⚠️ Вы получили предупреждение <b>{min(warn_count, 3)}/3</b>."
            )

            log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"лимит 24 часа (попытка {warn_count}){edit_tag}")

            if warn_count > 3:
                permit_remove(chat_id, uid)
                cooldown_warn_reset(chat_id, uid)

                info = (
                    f"🚫 <b>Разрешение снято</b>\n\n"
                    f"Пользователь: {user_mention}\n"
                    f"Причина: <b>нарушение правил пиара</b>\n"
                    f"(слишком много попыток рекламы раньше 24 часов){edit_tag}"
                )

                kb = kb_regrant(chat_id, uid)
                await bot.send_message(chat_id, info, reply_markup=kb)
                await notify_admins("⚠️ " + info, kb)

            return

        # можно отправлять — засчитываем использование рекламы
        permit_touch_last_ad(chat_id, uid)
        cooldown_warn_reset(chat_id, uid)
        return

    # (4) нет разрешения и реклама — стадии
    if (not permit_ok) and ad:
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)

        stage = ad_stage_get(chat_id, uid)

        if stage == 0:
            ad_stage_set(chat_id, uid, 1)

            if AD_WARN_STICKER_ID:
                try:
                    await bot.send_sticker(chat_id, AD_WARN_STICKER_ID)
                except Exception:
                    pass

            await bot.send_message(
                chat_id,
                f"{user_mention}, ваше сообщение удалено{edit_tag}.\n"
                f"Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Получить разрешение можно в боте: {SUPPORT_BOT_FOR_PERMIT}\n"
                f'В разделе "Связь с админом".'
            )

        elif stage == 1:
            ad_stage_set(chat_id, uid, 2)
            try:
                await apply_mute(chat_id, uid, MUTE_2_SECONDS)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                f"🔇 {user_mention} — мут на <b>3 часа</b>{edit_tag}.\n"
                f"Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Получить разрешение можно в боте: {SUPPORT_BOT_FOR_PERMIT}\n"
                f'В разделе "Связь с админом".'
            )
        else:
            ad_stage_set(chat_id, uid, 0)
            try:
                await apply_mute(chat_id, uid, MUTE_3_SECONDS)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                f"🔇 {user_mention} — мут на <b>12 часов</b>{edit_tag}.\n"
                f"Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Получить разрешение можно в боте: {SUPPORT_BOT_FOR_PERMIT}\n"
                f'В разделе "Связь с админом".\n\n'
                f"✅ Счётчик нарушений сброшен."
            )

        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"реклама без разрешения ({reason_detail}){edit_tag}")
        return


# =========================
# ГРУППА: АНТИ-РЕКЛАМА (новые сообщения)
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}) & (F.text | F.caption))
async def anti_ads(msg: Message):
    await handle_ad_check(msg, edited=False)

# =========================
# ГРУППА: АНТИ-РЕКЛАМА (редактирование)
# =========================
@dp.edited_message(F.chat.type.in_({"group", "supergroup"}) & (F.text | F.caption))
async def anti_ads_edited(msg: Message):
    await handle_ad_check(msg, edited=True)


# =========================
# Команды для подсказок "/"
# =========================
async def setup_commands():
    private_cmds = [
        BotCommand(command="start", description="Меню бота"),
        BotCommand(command="cancel", description="Отмена/выход в меню"),
        BotCommand(command="userid", description="Узнать ID (reply/forward/свой)"),
        BotCommand(command="chatid", description="Показать chat_id (в группе)"),
    ]

    await bot.set_my_commands(private_cmds, scope=BotCommandScopeAllPrivateChats())
    await bot.set_my_commands(private_cmds, scope=BotCommandScopeDefault())

    # убираем подсказки в группах
    await bot.set_my_commands([], scope=BotCommandScopeAllGroupChats())


# =========================
# MAIN
# =========================
async def main():
    db().close()
    await setup_commands()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
