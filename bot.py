import os
import re
import sqlite3
import asyncio
from datetime import datetime, timedelta, timezone

from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatPermissions
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext


# =========================
# НАСТРОЙКИ (ENV)
# =========================
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN in Render.")

# ADMIN_IDS в Render (опционально), иначе берём дефолт:
# ADMIN_IDS = "6911558950,8085895186"
_admins_env = os.environ.get("ADMIN_IDS", "6911558950,8085895186")
ADMIN_IDS = {int(x.strip()) for x in _admins_env.split(",") if x.strip().isdigit()}

DB_PATH = "mc_bot.db"

HASHTAG = "#реклама"
RULES_URL = "https://leoned777.github.io/chats/"
PERMIT_HELP = "@minecraft_chat_igra_bot"

# Логи: 5 действий на страницу
LOGS_PAGE_SIZE = 5

# /mclist: по 10 записей на страницу
MCLIST_PAGE_SIZE = 10

# реклама с разрешением: 1 раз в 24 часа
AD_COOLDOWN_SECONDS = 24 * 60 * 60

# авто-наказания рекламы (бот)
AUTO_MUTE_2_SECONDS = 3 * 60 * 60    # 3 часа
AUTO_MUTE_3_SECONDS = 12 * 60 * 60   # 12 часов

# /mcwarn: 4/3 -> бан 3 дня
MCWARN_BAN_SECONDS = 3 * 24 * 60 * 60


# =========================
# ВЕБ-ХЕЛСКЧЕК / UPTIMEROBOT
# =========================
async def health(request):
    return web.Response(text="OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Health server started on 0.0.0.0:{port}")


# =========================
# УТИЛИТЫ
# =========================
def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def ts_to_local_str(ts: int | None) -> str:
    if ts is None:
        return "Навсегда"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
    return dt.strftime("%d.%m.%Y %H:%M")

def active_tag(ts: int | None) -> str:
    if ts is None:
        return "[Активно]"
    return "[Активно]" if ts > now_ts() else "[Неактивно]"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def user_click_html(user_id: int, full_name: str | None, username: str | None) -> str:
    """
    Кликабельное имя:
      - если есть username -> https://t.me/username
      - иначе -> tg://user?id=ID
    """
    name = html_escape(full_name or f"User {user_id}")
    if username:
        uname = username.lstrip("@")
        return f'<a href="https://t.me/{uname}">{name}</a>'
    return f'<a href="tg://user?id={user_id}">{name}</a>'

def parse_duration(token: str | None) -> int | None:
    """
    15m / 2h / 1d / 1w / 1y
    None -> None (навсегда)
    """
    if not token:
        return None
    token = token.strip().lower()
    m = re.fullmatch(r"(\d{1,6})(m|h|d|w|y)", token)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    mult = {
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 7 * 86400,
        "y": 365 * 86400
    }[unit]
    return n * mult

def cut(s: str, limit: int = 900) -> str:
    s = s or ""
    return s if len(s) <= limit else s[:limit] + "…"

def normalize_text(s: str | None) -> str:
    return (s or "").strip()


# =========================
# РЕКЛАМА: критерии (username НЕ считается рекламой)
# =========================
RE_TG_LINK = re.compile(r"(https?://)?(t\.me|telegram\.me)/[A-Za-z0-9_]{3,}", re.IGNORECASE)
RE_HTTP = re.compile(r"https?://", re.IGNORECASE)
RE_PHONE = re.compile(r"(?<!\w)(\+?\d[\d\-\s\(\)]{8,}\d)(?!\w)")
KEYWORDS = ["сдам", "продам", "куплю", "прайс", "подпишитесь"]

def detect_ad_reason(text: str) -> tuple[bool, str]:
    t = (text or "").lower()

    if RE_TG_LINK.search(t) or RE_HTTP.search(t):
        return True, "ссылка"

    m = RE_PHONE.search(t)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) >= 10:
            return True, "номер телефона"

    for kw in KEYWORDS:
        if kw in t:
            return True, f"ключевое слово: {kw}"

    return False, ""

def has_hashtag_anywhere(text: str) -> bool:
    return HASHTAG in (text or "").lower()

def hashtag_at_end(text: str) -> bool:
    return bool(re.search(r"#реклама\s*$", (text or "").lower()))


# =========================
# БД
# =========================
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS chats(
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            updated_ts INTEGER NOT NULL
        )
    """)

    # кэш пользователей (чтобы @username работал)
    con.execute("""
        CREATE TABLE IF NOT EXISTS user_cache(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            updated_ts INTEGER NOT NULL
        )
    """)

    # разрешения рекламы
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_permits(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            until_ts INTEGER, -- NULL = навсегда
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    # лимит 24ч при разрешении
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_last_sent(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            last_ts INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    # авто-страйки рекламы (бот)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_strikes(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    # логи удалений рекламы
    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            chat_title TEXT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            msg_text TEXT,
            reason TEXT,
            created_ts INTEGER NOT NULL
        )
    """)

    # ручные наказания (перезаписываются)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mc_punishments(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            p_type TEXT NOT NULL,      -- WARN/MUTE/BAN/KICK (UN... не храним как актив)
            issued_ts INTEGER NOT NULL,
            until_ts INTEGER,          -- NULL = навсегда
            reason TEXT,
            actor_id INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id, p_type)
        )
    """)

    # счётчик /mcwarn (админы): 1..4 (4/3 -> бан 3 дня)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mc_warn_counter(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    # техподдержка
    con.execute("""
        CREATE TABLE IF NOT EXISTS support_tickets(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            opened_ts INTEGER NOT NULL,
            is_open INTEGER NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS support_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            sender TEXT NOT NULL, -- user/admin
            admin_id INTEGER,
            text TEXT,
            created_ts INTEGER NOT NULL
        )
    """)

    con.commit()
    return con

def upsert_chat(chat_id: int, title: str | None):
    con = db()
    con.execute("""
        INSERT INTO chats(chat_id, title, updated_ts)
        VALUES (?,?,?)
        ON CONFLICT(chat_id) DO UPDATE SET
            title=excluded.title,
            updated_ts=excluded.updated_ts
    """, (chat_id, title or "", now_ts()))
    con.commit()
    con.close()

def list_chats() -> list[tuple[int, str]]:
    con = db()
    rows = con.execute("SELECT chat_id, COALESCE(title, '') FROM chats ORDER BY updated_ts DESC").fetchall()
    con.close()
    out = []
    for cid, title in rows:
        out.append((int(cid), title if title else str(cid)))
    return out

def cache_user(user_id: int, username: str | None, full_name: str | None):
    con = db()
    con.execute("""
        INSERT INTO user_cache(user_id, username, full_name, updated_ts)
        VALUES (?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name,
            updated_ts=excluded.updated_ts
    """, (user_id, (username or "").lower() if username else None, full_name, now_ts()))
    con.commit()
    con.close()

def resolve_username_to_id(username: str) -> int | None:
    uname = username.lstrip("@").lower()
    con = db()
    row = con.execute(
        "SELECT user_id FROM user_cache WHERE username=? LIMIT 1",
        (uname,)
    ).fetchone()
    con.close()
    return int(row[0]) if row else None

def get_user_cached(user_id: int) -> tuple[str | None, str | None]:
    con = db()
    row = con.execute("SELECT username, full_name FROM user_cache WHERE user_id=? LIMIT 1", (user_id,)).fetchone()
    con.close()
    if not row:
        return None, None
    return row[0], row[1]


# --- permits
def permit_get(chat_id: int, user_id: int):
    con = db()
    row = con.execute("SELECT until_ts FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return row[0] if row else None  # None also means "no record" — проверим отдельно

def permit_exists(chat_id: int, user_id: int) -> bool:
    con = db()
    row = con.execute("SELECT 1 FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return row is not None

def permit_active(chat_id: int, user_id: int) -> bool:
    if not permit_exists(chat_id, user_id):
        return False
    until = permit_get(chat_id, user_id)
    if until is None:
        return True
    return until > now_ts()

def permit_set(chat_id: int, user_id: int, until_ts: int | None):
    con = db()
    con.execute("""
        INSERT INTO ad_permits(chat_id, user_id, until_ts)
        VALUES (?,?,?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET until_ts=excluded.until_ts
    """, (chat_id, user_id, until_ts))
    con.commit()
    con.close()

def permit_remove(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.execute("DELETE FROM ad_last_sent WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

# --- ad cooldown
def ad_last_get(chat_id: int, user_id: int) -> int | None:
    con = db()
    row = con.execute("SELECT last_ts FROM ad_last_sent WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else None

def ad_last_set(chat_id: int, user_id: int, ts: int):
    con = db()
    con.execute("""
        INSERT INTO ad_last_sent(chat_id, user_id, last_ts)
        VALUES (?,?,?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET last_ts=excluded.last_ts
    """, (chat_id, user_id, ts))
    con.commit()
    con.close()

# --- strikes
def strikes_get(chat_id: int, user_id: int) -> int:
    con = db()
    row = con.execute("SELECT strikes FROM ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else 0

def strikes_set(chat_id: int, user_id: int, strikes: int):
    con = db()
    con.execute("""
        INSERT INTO ad_strikes(chat_id, user_id, strikes)
        VALUES (?,?,?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET strikes=excluded.strikes
    """, (chat_id, user_id, strikes))
    con.commit()
    con.close()

def strikes_reset(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

# --- logs
def log_ad_delete(chat_id: int, chat_title: str, user_id: int, username: str | None, full_name: str | None,
                  msg_text: str, reason: str):
    con = db()
    con.execute("""
        INSERT INTO ad_logs(chat_id, chat_title, user_id, username, full_name, msg_text, reason, created_ts)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        chat_id, chat_title or "",
        user_id,
        (username or "").lower() if username else "",
        full_name or "",
        (msg_text or "")[:2000],
        reason,
        now_ts()
    ))
    con.commit()
    con.close()

def ad_logs_page(chat_id: int, page: int):
    offset = (page - 1) * LOGS_PAGE_SIZE
    con = db()
    rows = con.execute("""
        SELECT user_id, username, full_name, msg_text, reason, created_ts
        FROM ad_logs
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (chat_id, LOGS_PAGE_SIZE, offset)).fetchall()
    total = con.execute("SELECT COUNT(1) FROM ad_logs WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, int(total)

# --- mc warn counter
def mc_warn_get(chat_id: int, user_id: int) -> int:
    con = db()
    row = con.execute("SELECT warns FROM mc_warn_counter WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return int(row[0]) if row else 0

def mc_warn_set(chat_id: int, user_id: int, warns: int):
    con = db()
    con.execute("""
        INSERT INTO mc_warn_counter(chat_id, user_id, warns)
        VALUES (?,?,?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET warns=excluded.warns
    """, (chat_id, user_id, warns))
    con.commit()
    con.close()

def mc_warn_reset(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM mc_warn_counter WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()

# --- mc punishments
def mc_set_punishment(chat_id: int, user_id: int, p_type: str, until_ts: int | None, reason: str, actor_id: int):
    con = db()
    con.execute("""
        INSERT INTO mc_punishments(chat_id, user_id, p_type, issued_ts, until_ts, reason, actor_id)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(chat_id, user_id, p_type) DO UPDATE SET
            issued_ts=excluded.issued_ts,
            until_ts=excluded.until_ts,
            reason=excluded.reason,
            actor_id=excluded.actor_id
    """, (chat_id, user_id, p_type, now_ts(), until_ts, reason, actor_id))
    con.commit()
    con.close()

def mc_remove_punishment(chat_id: int, user_id: int, p_type: str):
    con = db()
    con.execute("DELETE FROM mc_punishments WHERE chat_id=? AND user_id=? AND p_type=?", (chat_id, user_id, p_type))
    con.commit()
    con.close()

def mc_list_page(chat_id: int, page: int):
    """
    Показываем последние записи по наказаниям (WARN/MUTE/BAN/KICK).
    """
    offset = (page - 1) * MCLIST_PAGE_SIZE
    con = db()
    rows = con.execute("""
        SELECT user_id, p_type, issued_ts, until_ts, reason
        FROM mc_punishments
        WHERE chat_id=?
        ORDER BY issued_ts DESC
        LIMIT ? OFFSET ?
    """, (chat_id, MCLIST_PAGE_SIZE, offset)).fetchall()
    total = con.execute("SELECT COUNT(1) FROM mc_punishments WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, int(total)


# --- support
def support_open(user_id: int, username: str | None, full_name: str | None):
    con = db()
    con.execute("""
        INSERT INTO support_tickets(user_id, username, full_name, opened_ts, is_open)
        VALUES (?,?,?,?,1)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name,
            is_open=1
    """, (user_id, username or "", full_name or "", now_ts()))
    con.commit()
    con.close()

def support_close(user_id: int):
    con = db()
    con.execute("UPDATE support_tickets SET is_open=0 WHERE user_id=?", (user_id,))
    con.commit()
    con.close()

def support_is_open(user_id: int) -> bool:
    con = db()
    row = con.execute("SELECT is_open FROM support_tickets WHERE user_id=?", (user_id,)).fetchone()
    con.close()
    return bool(row and int(row[0]) == 1)

def support_msg_add(user_id: int, sender: str, text: str, admin_id: int | None = None):
    con = db()
    con.execute("""
        INSERT INTO support_messages(user_id, sender, admin_id, text, created_ts)
        VALUES (?,?,?,?,?)
    """, (user_id, sender, admin_id, text, now_ts()))
    con.commit()
    con.close()


# =========================
# FSM
# =========================
class DmState(StatesGroup):
    # Разрешения (в ЛС)
    perm_wait_target = State()        # ожидаем ввод ID/@/пересылку
    perm_wait_action = State()        # give/remove
    perm_wait_chat = State()          # выбран чат

class BroadcastState(StatesGroup):
    choose_chat = State()
    enter_text = State()
    confirm = State()

class SupportReplyState(StatesGroup):
    waiting_reply = State()


# =========================
# КНОПКИ (ЛС)
# =========================
def kb_main(user_id: int) -> InlineKeyboardMarkup:
    rows = []

    # всем
    rows.append([InlineKeyboardButton(text="🆔 Узнать ID", callback_data="dm:myid")])
    rows.append([InlineKeyboardButton(text="💬 Связаться с администратором", callback_data="dm:support")])
    rows.append([InlineKeyboardButton(text="💎 VIP подписка", callback_data="dm:vip")])

    # админам
    if is_admin(user_id):
        rows.append([
            InlineKeyboardButton(text="👤 Профиль", callback_data="dm:profile"),
            InlineKeyboardButton(text="✅ Разрешения", callback_data="dm:perm"),
        ])
        rows.append([InlineKeyboardButton(text="🧾 Логи", callback_data="dm:logs")])
        rows.append([InlineKeyboardButton(text="📣 Рассылка", callback_data="dm:broadcast")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_back(cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=cb)]
    ])

def kb_perm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать", callback_data="dm:perm:give")],
        [InlineKeyboardButton(text="➖ Забрать", callback_data="dm:perm:remove")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="dm:home")]
    ])

def kb_chats(prefix: str, back: str) -> InlineKeyboardMarkup:
    chats = list_chats()
    kb = []
    for chat_id, title in chats[:25]:
        kb.append([InlineKeyboardButton(text=title, callback_data=f"{prefix}:{chat_id}")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_logs_nav(chat_id: int, page: int, total_pages: int) -> InlineKeyboardMarkup:
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"dm:logs:chat:{chat_id}:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"dm:logs:chat:{chat_id}:{page+1}"))
    kb = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton(text="⬅️ К чатам", callback_data="dm:logs")])
    kb.append([InlineKeyboardButton(text="🏠 Меню", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_broadcast_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="dm:bc:send"),
         InlineKeyboardButton(text="❌ Отмена", callback_data="dm:home")]
    ])

def kb_support_admin_actions(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Ответить", callback_data=f"sup:reply:{user_id}")],
        [InlineKeyboardButton(text="✅ Закрыть", callback_data=f"sup:close:{user_id}")]
    ])

def kb_mclist_nav(page: int, total_pages: int) -> InlineKeyboardMarkup:
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"mclist:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"mclist:{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[row]) if row else InlineKeyboardMarkup(inline_keyboard=[])


# =========================
# HELP (подсказки как у ириса)
# =========================
HELP_TEXT = (
    "📌 Подсказки по командам:\n\n"
    "<b>Разрешения рекламы</b>\n"
    "• /adgive <id|@user> [15m|2h|1d|1w|1y] — выдать (без времени = навсегда)\n"
    "• /adremove <id|@user> — забрать\n"
    "Можно ответом на сообщение: <code>/adgive 1d</code>\n\n"
    "<b>Наказания (админы)</b>\n"
    "• /mcwarn <id|@user> [причина]\n"
    "  (счётчик админ-предупреждений, при 4/3 = бан 3 дня)\n"
    "• /mcmute <id|@user> [время] [причина]  (без времени = навсегда)\n"
    "• /mcban  <id|@user> [время] [причина]  (без времени = навсегда)\n"
    "• /mckick <id|@user> [причина]\n\n"
    "<b>Снять</b>\n"
    "• /mcunwarn <id|@user>\n"
    "• /mcunmute <id|@user>\n"
    "• /mcunban  <id|@user>\n\n"
    "• /mclist [страница] — список наказаний\n"
)

# =========================
# БОТ
# =========================
bot = Bot(TOKEN, parse_mode="HTML")
dp = Dispatcher()


# =========================
# ВСПОМОГАТЕЛЬНО: сохранить чат/юзера
# =========================
def touch(msg: Message):
    if msg.from_user:
        cache_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    if msg.chat and msg.chat.type in ("group", "supergroup"):
        upsert_chat(msg.chat.id, msg.chat.title or str(msg.chat.id))


# =========================
# /start
# =========================
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    touch(msg)
    if msg.chat.type != "private":
        return await msg.reply("✅ Я в чате. Команды: /mchelp, /mclist")
    await state.clear()
    await msg.answer("🏠 Главное меню", reply_markup=kb_main(msg.from_user.id))

@dp.message(Command("mchelp"))
async def cmd_mchelp(msg: Message):
    touch(msg)
    await msg.reply(HELP_TEXT)

# =========================
# ЛС: меню
# =========================
@dp.callback_query(F.data == "dm:home")
async def dm_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("🏠 Главное меню", reply_markup=kb_main(c.from_user.id))
    await c.answer()

@dp.callback_query(F.data == "dm:myid")
async def dm_myid(c: CallbackQuery):
    await c.message.edit_text(f"🆔 Ваш Telegram ID: <code>{c.from_user.id}</code>", reply_markup=kb_back("dm:home"))
    await c.answer()

@dp.callback_query(F.data == "dm:vip")
async def dm_vip(c: CallbackQuery):
    await c.message.edit_text("💎 VIP подписка\n\n(позже добавим)", reply_markup=kb_back("dm:home"))
    await c.answer()

@dp.callback_query(F.data == "dm:profile")
async def dm_profile(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await c.message.edit_text(
        f"👤 Профиль\n\n"
        f"Статус: <b>Админ</b>\n"
        f"ID: <code>{c.from_user.id}</code>\n",
        reply_markup=kb_back("dm:home")
    )
    await c.answer()

# =========================
# ЛС: разрешения
# =========================
@dp.callback_query(F.data == "dm:perm")
async def dm_perm(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await c.message.edit_text(
        "✅ Разрешения на рекламу\n\n"
        "Можно указать:\n"
        "• ID\n"
        "• пересланное сообщение\n"
        "• @username (если бот видел пользователя)\n",
        reply_markup=kb_perm_menu()
    )
    await c.answer()

@dp.callback_query(F.data == "dm:perm:give")
async def dm_perm_give(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await state.update_data(perm_action="give")
    await c.message.edit_text("Выберите чат:", reply_markup=kb_chats("dm:perm:chat", "dm:perm"))
    await c.answer()

@dp.callback_query(F.data == "dm:perm:remove")
async def dm_perm_remove(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await state.update_data(perm_action="remove")
    await c.message.edit_text("Выберите чат:", reply_markup=kb_chats("dm:perm:chat", "dm:perm"))
    await c.answer()

@dp.callback_query(F.data.startswith("dm:perm:chat:"))
async def dm_perm_chat_selected(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)

    chat_id = int(c.data.split(":")[3])
    data = await state.get_data()
    action = data.get("perm_action")
    if action not in ("give", "remove"):
        await state.clear()
        return await c.message.edit_text("Ошибка состояния.", reply_markup=kb_main(c.from_user.id))

    await state.update_data(perm_chat_id=chat_id)
    await state.set_state(DmState.perm_wait_target)

    if action == "give":
        txt = (
            "➕ Выдать разрешение\n\n"
            "Отправьте:\n"
            "• ID (число)\n"
            "• @username\n"
            "• или перешлите сообщение пользователя\n\n"
            "После этого я спрошу срок (например 15m/2h/1d/1w/1y) или оставьте пустым = навсегда."
        )
    else:
        txt = (
            "➖ Забрать разрешение\n\n"
            "Отправьте:\n"
            "• ID (число)\n"
            "• @username\n"
            "• или перешлите сообщение пользователя"
        )

    await c.message.edit_text(txt, reply_markup=kb_back("dm:perm"))
    await c.answer()

def extract_forward_id(msg: Message) -> int | None:
    # если Telegram не скрывает автора, forward_from будет
    if msg.forward_from:
        return msg.forward_from.id
    return None

async def dm_resolve_target_from_input(msg: Message) -> tuple[int | None, str | None]:
    """
    Возвращает (user_id, username_or_none).
    В ЛС: поддержка ID, @username (если видели), forward_from (если не скрыто).
    """
    if msg.text:
        t = msg.text.strip()
        if t.isdigit():
            return int(t), None
        if t.startswith("@"):
            uid = resolve_username_to_id(t)
            if uid is None:
                return None, t
            return uid, t.lstrip("@").lower()

    fwd = extract_forward_id(msg)
    if fwd is not None:
        return fwd, None

    return None, None

@dp.message(DmState.perm_wait_target)
async def dm_perm_wait_target(msg: Message, state: FSMContext):
    touch(msg)
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    action = data.get("perm_action")
    chat_id = int(data.get("perm_chat_id", 0))
    if not chat_id or action not in ("give", "remove"):
        await state.clear()
        return await msg.answer("Ошибка состояния.", reply_markup=kb_main(msg.from_user.id))

    uid, uname = await dm_resolve_target_from_input(msg)
    if uid is None:
        if uname and uname.startswith("@"):
            return await msg.answer(
                "❌ Я не знаю ID этого @username (бот его ещё не видел).\n\n"
                "Как исправить:\n"
                "1) Пусть пользователь напишет боту /start\n"
                "2) Или перешлите сообщение пользователя (если не скрыт автор)\n"
                "3) Или пришлите ID числом",
                reply_markup=kb_back("dm:perm")
            )
        return await msg.answer(
            "❌ Не смог определить пользователя.\n"
            "Пришлите ID / @username / пересланное сообщение.",
            reply_markup=kb_back("dm:perm")
        )

    await state.update_data(perm_target_id=uid)

    if action == "remove":
        permit_remove(chat_id, uid)
        await state.clear()
        return await msg.answer(
            f"✅ Разрешение снято.\nЧат: <code>{chat_id}</code>\nПользователь: <code>{uid}</code>",
            reply_markup=kb_main(msg.from_user.id)
        )

    # give -> спрашиваем срок
    await state.set_state(DmState.perm_wait_action)
    await msg.answer(
        f"Пользователь: <code>{uid}</code>\n"
        f"Введите срок (например 15m/2h/1d/1w/1y) или отправьте <b>0</b> = навсегда:",
        reply_markup=kb_back("dm:perm")
    )

@dp.message(DmState.perm_wait_action)
async def dm_perm_wait_duration(msg: Message, state: FSMContext):
    touch(msg)
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    chat_id = int(data.get("perm_chat_id", 0))
    uid = int(data.get("perm_target_id", 0))
    if not chat_id or not uid:
        await state.clear()
        return await msg.answer("Ошибка состояния.", reply_markup=kb_main(msg.from_user.id))

    t = (msg.text or "").strip().lower()
    if t == "0" or t == "":
        until_ts = None
    else:
        sec = parse_duration(t)
        if sec is None:
            return await msg.answer(
                "❌ Неверный формат.\nПримеры: 15m, 2h, 1d, 1w, 1y\nИли 0 = навсегда.",
                reply_markup=kb_back("dm:perm")
            )
        until_ts = now_ts() + sec

    permit_set(chat_id, uid, until_ts)
    await state.clear()
    await msg.answer(
        f"✅ Разрешение выдано.\n"
        f"Чат: <code>{chat_id}</code>\n"
        f"Пользователь: <code>{uid}</code>\n"
        f"До: <b>{ts_to_local_str(until_ts)}</b> {active_tag(until_ts)}",
        reply_markup=kb_main(msg.from_user.id)
    )


# =========================
# ЛС: логи рекламы
# =========================
@dp.callback_query(F.data == "dm:logs")
async def dm_logs(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await c.message.edit_text("🧾 Логи рекламы — выберите чат:", reply_markup=kb_chats("dm:logs:pick", "dm:home"))
    await c.answer()

@dp.callback_query(F.data.startswith("dm:logs:pick:"))
async def dm_logs_pick(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    chat_id = int(c.data.split(":")[3])
    await show_logs_page(c.message, chat_id, 1)
    await c.answer()

@dp.callback_query(F.data.startswith("dm:logs:chat:"))
async def dm_logs_page(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    _, _, _, chat_id_s, page_s = c.data.split(":")
    await show_logs_page(c.message, int(chat_id_s), int(page_s))
    await c.answer()

async def show_logs_page(msg, chat_id: int, page: int):
    rows, total = ad_logs_page(chat_id, page)
    total_pages = max(1, (total + LOGS_PAGE_SIZE - 1) // LOGS_PAGE_SIZE)

    text = f"🧾 Логи рекламы\nЧат: <code>{chat_id}</code>\nСтраница {page}/{total_pages}\n\n"
    if not rows:
        text += "Пока нет удалений."
    else:
        for (uid, username, full_name, msg_text, reason, ts) in rows:
            click = user_click_html(uid, full_name, username if username else None)
            text += (
                f"🗑 <b>Удалено</b>\n"
                f"👤 {click} | <code>{uid}</code>\n"
                f"🕒 {ts_to_local_str(ts)}\n"
                f"📌 Причина: <b>{html_escape(reason)}</b>\n"
                f"💬 {html_escape(cut(msg_text, 450))}\n"
                f"— — —\n"
            )

    await msg.edit_text(text, reply_markup=kb_logs_nav(chat_id, page, total_pages))


# =========================
# ЛС: рассылка (админы)
# =========================
@dp.callback_query(F.data == "dm:broadcast")
async def dm_broadcast(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await c.message.edit_text("📣 Рассылка — выберите чат:", reply_markup=kb_chats("dm:bc:chat", "dm:home"))
    await state.set_state(BroadcastState.choose_chat)
    await c.answer()

@dp.callback_query(BroadcastState.choose_chat, F.data.startswith("dm:bc:chat:"))
async def dm_bc_choose_chat(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    chat_id = int(c.data.split(":")[3])
    await state.update_data(bc_chat_id=chat_id)
    await c.message.edit_text(
        f"📣 Рассылка в чат <code>{chat_id}</code>\n\nОтправьте текст сообщения:",
        reply_markup=kb_back("dm:home")
    )
    await state.set_state(BroadcastState.enter_text)
    await c.answer()

@dp.message(BroadcastState.enter_text, F.text)
async def dm_bc_enter_text(msg: Message, state: FSMContext):
    touch(msg)
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        return

    data = await state.get_data()
    chat_id = data.get("bc_chat_id")
    text = msg.text or ""
    await state.update_data(bc_text=text)

    await msg.answer(
        f"✅ Подтверждение\n\nЧат: <code>{chat_id}</code>\nСообщение:\n{html_escape(text)}",
        reply_markup=kb_broadcast_confirm()
    )
    await state.set_state(BroadcastState.confirm)

@dp.callback_query(BroadcastState.confirm, F.data == "dm:bc:send")
async def dm_bc_send(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)

    data = await state.get_data()
    chat_id = int(data.get("bc_chat_id", 0))
    text = data.get("bc_text") or ""

    try:
        await bot.send_message(chat_id, text)
        await c.message.edit_text("✅ Рассылка отправлена.", reply_markup=kb_back("dm:home"))
    except Exception as e:
        await c.message.edit_text(f"❌ Ошибка отправки: {html_escape(str(e))}", reply_markup=kb_back("dm:home"))

    await state.clear()
    await c.answer()


# =========================
# ТЕХПОДДЕРЖКА (всем)
# =========================
@dp.callback_query(F.data == "dm:support")
async def dm_support_open(c: CallbackQuery):
    support_open(c.from_user.id, c.from_user.username, c.from_user.full_name)
    cache_user(c.from_user.id, c.from_user.username, c.from_user.full_name)

    await c.message.edit_text(
        "💬 Связаться с администратором\n\n"
        "Напишите сюда сообщение — я отправлю его админам.\n"
        "Чтобы закрыть диалог: /close",
        reply_markup=kb_back("dm:home")
    )
    await c.answer()

@dp.message(Command("close"))
async def dm_support_close_cmd(msg: Message):
    touch(msg)
    if msg.chat.type != "private":
        return
    support_close(msg.from_user.id)
    await msg.answer("✅ Диалог закрыт.", reply_markup=kb_main(msg.from_user.id))

@dp.callback_query(F.data.startswith("sup:reply:"))
async def sup_reply_cb(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    user_id = int(c.data.split(":")[2])

    await state.clear()
    await state.set_state(SupportReplyState.waiting_reply)
    await state.update_data(reply_to_user_id=user_id)

    await c.message.reply(
        f"✉️ Ответ пользователю <code>{user_id}</code>\n"
        f"Напишите сообщение — я отправлю.",
    )
    await c.answer()

@dp.callback_query(F.data.startswith("sup:close:"))
async def sup_close_cb(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    user_id = int(c.data.split(":")[2])
    support_close(user_id)
    await c.message.reply(f"✅ Тикет закрыт для <code>{user_id}</code>")
    await c.answer()

@dp.message(F.chat.type == "private", F.text)
async def dm_text_router(msg: Message, state: FSMContext):
    touch(msg)

    # режим ответа админа
    if await state.get_state() == SupportReplyState.waiting_reply:
        data = await state.get_data()
        user_id = int(data.get("reply_to_user_id", 0))
        if not user_id:
            await state.clear()
            return await msg.answer("Ошибка: не выбран пользователь.", reply_markup=kb_main(msg.from_user.id))

        text = msg.text or ""
        support_msg_add(user_id, "admin", text, admin_id=msg.from_user.id)
        try:
            await bot.send_message(user_id, f"✉️ Ответ администратора:\n\n{text}")
            await msg.answer("✅ Отправлено.", reply_markup=kb_main(msg.from_user.id))
        except Exception:
            await msg.answer("❌ Не удалось отправить пользователю (он мог не запускать бота).", reply_markup=kb_main(msg.from_user.id))

        await state.clear()
        return

    # обычный пользователь — если тикет открыт, пересылаем админам
    if support_is_open(msg.from_user.id) and not is_admin(msg.from_user.id):
        text = msg.text or ""
        support_msg_add(msg.from_user.id, "user", text)

        click = user_click_html(msg.from_user.id, msg.from_user.full_name, msg.from_user.username)
        uname = f"@{msg.from_user.username}" if msg.from_user.username else "(без username)"

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"💬 <b>Сообщение в поддержку</b>\n"
                    f"От: {click} {uname}\n"
                    f"ID: <code>{msg.from_user.id}</code>\n\n"
                    f"{html_escape(text)}",
                    reply_markup=kb_support_admin_actions(msg.from_user.id)
                )
            except Exception:
                pass

        await msg.answer("✅ Сообщение отправлено администраторам.")
        return

    # если не тикет — просто молча игнорим, чтобы не мешать меню
    return


# =========================
# РАЗРЕШЕНИЯ (КОМАНДЫ В ЧАТЕ ИЛИ ЛС)
# =========================
async def resolve_target(msg: Message, maybe: str | None) -> int | None:
    # reply приоритет
    if msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user.id

    if not maybe:
        return None

    maybe = maybe.strip()
    if maybe.isdigit():
        return int(maybe)

    if maybe.startswith("@"):
        return resolve_username_to_id(maybe)

    return None

@dp.message(Command("adgive"))
async def cmd_adgive(msg: Message):
    touch(msg)
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()
    # /adgive @u 1d причина(не нужна) -> разрешение
    # /adgive 1d (reply)
    target = None
    dur = None

    if msg.reply_to_message and len(parts) >= 2 and parse_duration(parts[1]) is not None:
        dur = parts[1]
    else:
        target = parts[1] if len(parts) >= 2 else None
        dur = parts[2] if len(parts) >= 3 else None

    uid = await resolve_target(msg, target)
    if uid is None:
        return await msg.reply(
            "❌ Не могу определить пользователя.\n\n"
            "Примеры:\n"
            "• /adgive 123456789 1d\n"
            "• /adgive @nickname 1d  (если бот видел пользователя)\n"
            "• reply на сообщение + /adgive 1d\n"
            "• без времени: /adgive 123456789"
        )

    sec = parse_duration(dur)
    until_ts = None if sec is None else now_ts() + sec

    if msg.chat.type in ("group", "supergroup"):
        permit_set(msg.chat.id, uid, until_ts)
        await msg.reply(f"✅ Разрешение выдано: <code>{uid}</code>\nДо: <b>{ts_to_local_str(until_ts)}</b> {active_tag(until_ts)}")
    else:
        # в ЛС без выбора чата не делаем (есть меню)
        await msg.reply("ℹ️ Выдавай разрешения через меню бота: ✅ Разрешения")

@dp.message(Command("adremove"))
async def cmd_adremove(msg: Message):
    touch(msg)
    if not is_admin(msg.from_user.id):
        return

    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("ℹ️ Снимай разрешения через меню бота: ✅ Разрешения")

    parts = (msg.text or "").split()
    target = parts[1] if len(parts) >= 2 else None

    uid = await resolve_target(msg, target)
    if uid is None:
        return await msg.reply(
            "❌ Не могу определить пользователя.\n"
            "Пример: /adremove 123456789\n"
            "или reply + /adremove"
        )

    permit_remove(msg.chat.id, uid)
    await msg.reply(f"🗑️ Разрешение снято: <code>{uid}</code>")


# =========================
# АВТО-МОДЕРАЦИЯ РЕКЛАМЫ (текст + caption)
# =========================
async def mute(chat_id: int, user_id: int, seconds: int):
    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    perms = ChatPermissions(can_send_messages=False)
    await bot.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)

async def ban(chat_id: int, user_id: int, seconds: int | None):
    if seconds is None:
        await bot.ban_chat_member(chat_id, user_id)
        return None
    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    await bot.ban_chat_member(chat_id, user_id, until_date=until)
    return int(until.timestamp())

async def auto_ad_handle(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return

    text = msg.text or msg.caption or ""
    if not text:
        return

    is_ad, reason = detect_ad_reason(text)
    if not is_ad:
        return

    chat_id = msg.chat.id
    chat_title = msg.chat.title or str(chat_id)
    user = msg.from_user
    uid = user.id
    cache_user(uid, user.username, user.full_name)

    has_permit = permit_active(chat_id, uid)

    # НЕТ разрешения:
    if not has_permit:
        # удалить всегда
        try:
            await msg.delete()
        except Exception:
            pass

        # если человек написал #реклама без разрешения:
        if has_hashtag_anywhere(text):
            await bot.send_message(
                chat_id,
                f"🚫 {user_click_html(uid, user.full_name, user.username)}\n"
                f"Ваше сообщение удалено: <b>нет разрешения на рекламу</b>.\n"
                f"Получить разрешение: {PERMIT_HELP}"
            )
            log_ad_delete(chat_id, chat_title, uid, user.username, user.full_name, text, f"{reason} + был #реклама без разрешения")
            return

        # авто-стадии 1/3 2/3 3/3 + сброс
        strikes = strikes_get(chat_id, uid) + 1

        if strikes == 1:
            strikes_set(chat_id, uid, 1)
            await bot.send_message(
                chat_id,
                f"⚠️ {user_click_html(uid, user.full_name, user.username)}\n"
                f"Реклама без разрешения: <b>предупреждение</b> (1/3)\n"
                f"Причина: <b>{html_escape(reason)}</b>\n"
                f"Ознакомиться с правилами: {RULES_URL}\n"
                f"Получить разрешение: {PERMIT_HELP}"
            )
        elif strikes == 2:
            strikes_set(chat_id, uid, 2)
            try:
                await mute(chat_id, uid, AUTO_MUTE_2_SECONDS)
                await bot.send_message(
                    chat_id,
                    f"🔇 {user_click_html(uid, user.full_name, user.username)}\n"
                    f"Реклама без разрешения: <b>мут 3 часа</b> (2/3)\n"
                    f"Причина: <b>{html_escape(reason)}</b>\n"
                    f"Ознакомиться с правилами: {RULES_URL}\n"
                    f"Получить разрешение: {PERMIT_HELP}"
                )
            except Exception:
                await bot.send_message(chat_id, "❌ Не могу выдать мут — дай мне право «Ограничивать участников».")
        else:
            # 3-й раз
            try:
                await mute(chat_id, uid, AUTO_MUTE_3_SECONDS)
                await bot.send_message(
                    chat_id,
                    f"🔇 {user_click_html(uid, user.full_name, user.username)}\n"
                    f"Реклама без разрешения: <b>мут 12 часов</b> (3/3)\n"
                    f"⚠️ Счётчик предупреждений сброшен.\n"
                    f"Причина: <b>{html_escape(reason)}</b>\n"
                    f"Ознакомиться с правилами: {RULES_URL}\n"
                    f"Получить разрешение: {PERMIT_HELP}"
                )
            except Exception:
                await bot.send_message(chat_id, "❌ Не могу выдать мут — дай мне право «Ограничивать участников».")
            finally:
                strikes_reset(chat_id, uid)

        log_ad_delete(chat_id, chat_title, uid, user.username, user.full_name, text, reason)
        return

    # ЕСТЬ разрешение:
    # 24 часа лимит
    last_ts = ad_last_get(chat_id, uid)
    if last_ts and (now_ts() - last_ts) < AD_COOLDOWN_SECONDS:
        try:
            await msg.delete()
        except Exception:
            pass
        remaining = AD_COOLDOWN_SECONDS - (now_ts() - last_ts)
        hrs = remaining // 3600
        mins = (remaining % 3600) // 60
        await bot.send_message(
            chat_id,
            f"⏳ {user_click_html(uid, user.full_name, user.username)}\n"
            f"Рекламу можно публиковать <b>раз в 24 часа</b>.\n"
            f"Осталось: <b>{hrs}ч {mins}м</b>.\n"
            f'Не забудьте тег "<code>#реклама</code>" в конце.'
        )
        log_ad_delete(chat_id, chat_title, uid, user.username, user.full_name, text, "лимит 24ч")
        return

    # должен быть тег в конце
    if not hashtag_at_end(text):
        try:
            await msg.delete()
        except Exception:
            pass
        await bot.send_message(
            chat_id,
            f"🗑️ {user_click_html(uid, user.full_name, user.username)}\n"
            f"Ваше сообщение удалено, по причине отсутствия тега на рекламу.\n"
            f'Пожалуйста укажите в вашей рекламе тег "<code>#реклама</code>" в конце.'
        )
        log_ad_delete(chat_id, chat_title, uid, user.username, user.full_name, text, "нет #реклама в конце (при разрешении)")
        return

    # всё ок -> фиксируем последнюю рекламу (кулдаун)
    ad_last_set(chat_id, uid, now_ts())


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def group_any_message(msg: Message):
    touch(msg)
    # проверяем текст и подписи к медиа
    if not (msg.text or msg.caption):
        return
    await auto_ad_handle(msg)


# =========================
# MC-команды (админы) + кликабельное имя
# =========================
async def resolve_target_cmd(msg: Message, parts: list[str]) -> tuple[int | None, list[str]]:
    """
    Возвращает (target_user_id, remaining_parts_for_reason_and_time)
    Поддержка:
      - reply + /cmd 1d причина (без указания юзера)
      - /cmd @user 1d причина
      - /cmd 12345 1d причина
    """
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = msg.reply_to_message.from_user.id
        # если первым аргументом указан @/id — тогда он важнее
        if parts and (parts[0].isdigit() or parts[0].startswith("@")):
            tid = await resolve_target(msg, parts[0])
            if tid is not None:
                return tid, parts[1:]
        return target_id, parts

    if not parts:
        return None, []
    tid = await resolve_target(msg, parts[0])
    if tid is None:
        return None, parts
    return tid, parts[1:]

def reason_from(parts: list[str]) -> str:
    r = " ".join(parts).strip()
    return r if r else "не указана"

def time_and_reason(parts: list[str]) -> tuple[int | None, str]:
    """
    если первый токен похож на duration -> вернём seconds, остальное причина
    иначе seconds=None и вся строка причина
    """
    if parts:
        sec = parse_duration(parts[0])
        if sec is not None:
            return sec, reason_from(parts[1:])
    return None, reason_from(parts)

@dp.message(Command("mclist"))
async def cmd_mclist(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("Команда доступна только в группе.")

    parts = (msg.text or "").split()
    page = 1
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))

    rows, total = mc_list_page(msg.chat.id, page)
    if total == 0:
        return await msg.reply("Список наказаний пуст.")

    total_pages = max(1, (total + MCLIST_PAGE_SIZE - 1) // MCLIST_PAGE_SIZE)

    text = f"📋 /mclist — наказания (стр. {page}/{total_pages})\n\n"
    for (uid, p_type, issued_ts, until_ts, reason) in rows:
        uname, full = get_user_cached(uid)
        click = user_click_html(uid, full, uname)
        text += (
            f"{click} | <b>{p_type}</b>\n"
            f"Выдано: {ts_to_local_str(issued_ts)}\n"
            f"До: {ts_to_local_str(until_ts)} {active_tag(until_ts)}\n"
            f"Причина: {html_escape(reason or 'не указана')}\n"
            f"— — —\n"
        )

    await msg.reply(text, reply_markup=kb_mclist_nav(page, total_pages))

@dp.callback_query(F.data.startswith("mclist:"))
async def cb_mclist(c: CallbackQuery):
    if c.message.chat.type not in ("group", "supergroup"):
        return await c.answer()
    page = int(c.data.split(":")[1])
    rows, total = mc_list_page(c.message.chat.id, page)
    total_pages = max(1, (total + MCLIST_PAGE_SIZE - 1) // MCLIST_PAGE_SIZE)

    if total == 0:
        await c.message.edit_text("Список наказаний пуст.")
        return await c.answer()

    text = f"📋 /mclist — наказания (стр. {page}/{total_pages})\n\n"
    for (uid, p_type, issued_ts, until_ts, reason) in rows:
        uname, full = get_user_cached(uid)
        click = user_click_html(uid, full, uname)
        text += (
            f"{click} | <b>{p_type}</b>\n"
            f"Выдано: {ts_to_local_str(issued_ts)}\n"
            f"До: {ts_to_local_str(until_ts)} {active_tag(until_ts)}\n"
            f"Причина: {html_escape(reason or 'не указана')}\n"
            f"— — —\n"
        )

    await c.message.edit_text(text, reply_markup=kb_mclist_nav(page, total_pages))
    await c.answer()


@dp.message(Command("mcwarn"))
async def cmd_mcwarn(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply(
            "❌ Неверно.\n"
            "Пример: /mcwarn @user причина\n"
            "Или reply + /mcwarn причина\n"
        )

    reason = reason_from(rest)

    # счётчик warn админов
    warns = mc_warn_get(msg.chat.id, target_id) + 1

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)

    # 4/3 -> бан 3 дня сразу
    if warns >= 4:
        mc_warn_reset(msg.chat.id, target_id)
        until_ts = None
        try:
            until_ts = await ban(msg.chat.id, target_id, MCWARN_BAN_SECONDS)
        except Exception:
            return await msg.reply("❌ Не смог выдать бан. Проверь права бота: «Блокировать пользователей».")

        mc_set_punishment(msg.chat.id, target_id, "BAN", until_ts, f"авто-бан по предупреждениям (4/3). {reason}", msg.from_user.id)
        await msg.reply(
            f"⛔ {click}\n"
            f"Счётчик предупреждений: <b>4/3</b>\n"
            f"<b>Авто-бан на 3 дня</b>\n"
            f"До: <b>{ts_to_local_str(until_ts)}</b> {active_tag(until_ts)}\n"
            f"Причина: {html_escape(reason)}"
        )
        return

    mc_warn_set(msg.chat.id, target_id, warns)
    mc_set_punishment(msg.chat.id, target_id, "WARN", None, reason, msg.from_user.id)

    await msg.reply(
        f"⚠️ {click}\n"
        f"Выдано предупреждение. Счётчик: <b>{warns}/3</b>\n"
        f"Причина: {html_escape(reason)}"
    )

@dp.message(Command("mcunwarn"))
async def cmd_mcunwarn(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, _rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mcunwarn @user или reply + /mcunwarn")

    mc_warn_reset(msg.chat.id, target_id)
    mc_remove_punishment(msg.chat.id, target_id, "WARN")

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(f"✅ Сброс предупреждений: {click}")

@dp.message(Command("mcmute"))
async def cmd_mcmute(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mcmute @user 3h причина  или reply + /mcmute 3h причина")

    sec, reason = time_and_reason(rest)
    # если не указано время -> "навсегда" (ставим 365 дней)
    mute_seconds = sec if sec is not None else (365 * 86400)
    until = datetime.now(timezone.utc) + timedelta(seconds=mute_seconds)
    until_ts = int(until.timestamp())

    try:
        perms = ChatPermissions(can_send_messages=False)
        await bot.restrict_chat_member(msg.chat.id, target_id, permissions=perms, until_date=until)
    except Exception:
        return await msg.reply("❌ Не смог выдать мут. Проверь права бота: «Ограничивать участников».")

    mc_set_punishment(msg.chat.id, target_id, "MUTE", until_ts if sec is not None else None, reason, msg.from_user.id)

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(
        f"🔇 {click}\n"
        f"Мут выдан до: <b>{ts_to_local_str(until_ts if sec is not None else None)}</b> {active_tag(until_ts if sec is not None else None)}\n"
        f"Причина: {html_escape(reason)}"
    )

@dp.message(Command("mcunmute"))
async def cmd_mcunmute(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, _rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mcunmute @user или reply + /mcunmute")

    try:
        perms = ChatPermissions(
            can_send_messages=True,
            can_send_audios=True,
            can_send_documents=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_video_notes=True,
            can_send_voice_notes=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True,
        )
        await bot.restrict_chat_member(msg.chat.id, target_id, permissions=perms)
    except Exception:
        return await msg.reply("❌ Не смог снять мут. Проверь права бота.")

    mc_remove_punishment(msg.chat.id, target_id, "MUTE")

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(f"✅ Мут снят: {click}")

@dp.message(Command("mcban"))
async def cmd_mcban(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mcban @user 1d причина  или reply + /mcban 1d причина")

    sec, reason = time_and_reason(rest)
    try:
        until_ts = await ban(msg.chat.id, target_id, sec)
    except Exception:
        return await msg.reply("❌ Не смог выдать бан. Проверь права бота: «Блокировать пользователей».")

    mc_set_punishment(msg.chat.id, target_id, "BAN", until_ts if sec is not None else None, reason, msg.from_user.id)

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(
        f"⛔ {click}\n"
        f"Бан до: <b>{ts_to_local_str(until_ts if sec is not None else None)}</b> {active_tag(until_ts if sec is not None else None)}\n"
        f"Причина: {html_escape(reason)}"
    )

@dp.message(Command("mcunban"))
async def cmd_mcunban(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, _rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mcunban @user или reply + /mcunban")

    try:
        await bot.unban_chat_member(msg.chat.id, target_id)
    except Exception:
        return await msg.reply("❌ Не смог разбанить. Проверь права бота.")

    mc_remove_punishment(msg.chat.id, target_id, "BAN")

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(f"✅ Разбан: {click}")

@dp.message(Command("mckick"))
async def cmd_mckick(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, rest = await resolve_target_cmd(msg, parts)
    if target_id is None:
        return await msg.reply("❌ Пример: /mckick @user причина  или reply + /mckick причина")

    reason = reason_from(rest)

    try:
        # кик = ban на 60 сек + unban
        await bot.ban_chat_member(msg.chat.id, target_id, until_date=datetime.now(timezone.utc) + timedelta(seconds=60))
        await bot.unban_chat_member(msg.chat.id, target_id)
    except Exception:
        return await msg.reply("❌ Не смог кикнуть. Проверь права бота: «Блокировать пользователей».")

    mc_set_punishment(msg.chat.id, target_id, "KICK", None, reason, msg.from_user.id)

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)
    await msg.reply(f"👢 Кик: {click}\nПричина: {html_escape(reason)}")


# =========================
# MAIN
# =========================
async def main():
    db().close()
    await start_web_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
