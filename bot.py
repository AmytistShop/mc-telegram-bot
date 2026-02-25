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
from aiogram.client.default import DefaultBotProperties


# =========================
# НАСТРОЙКИ (ENV)
# =========================
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN in Render.")

# ADMIN_IDS можно тоже вынести в ENV, но оставляю твои:
# ADMIN_IDS="6911558950,8085895186"
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

    # кэш пользователей (для @username в разрешениях)
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

    # ручные наказания (история, для /mclist)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mc_punishments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            p_type TEXT NOT NULL,      -- WARN/MUTE/BAN/KICK/UN...
            issued_ts INTEGER NOT NULL,
            until_ts INTEGER,          -- NULL = навсегда или не применимо
            reason TEXT,
            actor_id INTEGER NOT NULL
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
    return [(int(cid), title if title else str(cid)) for cid, title in rows]

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
def permit_exists(chat_id: int, user_id: int) -> bool:
    con = db()
    row = con.execute("SELECT 1 FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return row is not None

def permit_get(chat_id: int, user_id: int):
    con = db()
    row = con.execute("SELECT until_ts FROM ad_permits WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
    con.close()
    return row[0] if row else None

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

# --- mc punish history
def mc_log(chat_id: int, user_id: int, p_type: str, until_ts: int | None, reason: str, actor_id: int):
    con = db()
    con.execute("""
        INSERT INTO mc_punishments(chat_id, user_id, p_type, issued_ts, until_ts, reason, actor_id)
        VALUES (?,?,?,?,?,?,?)
    """, (chat_id, user_id, p_type, now_ts(), until_ts, reason, actor_id))
    con.commit()
    con.close()

def mc_list_page(chat_id: int, page: int):
    offset = (page - 1) * MCLIST_PAGE_SIZE
    con = db()
    rows = con.execute("""
        SELECT user_id, p_type, issued_ts, until_ts, reason
        FROM mc_punishments
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (chat_id, MCLIST_PAGE_SIZE, offset)).fetchall()
    total = con.execute("SELECT COUNT(1) FROM mc_punishments WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, int(total)


# =========================
# FSM (ЛС меню)
# =========================
class DmState(StatesGroup):
    perm_wait_target = State()
    perm_wait_action = State()  # give duration
    perm_wait_chat = State()


# =========================
# КНОПКИ (ЛС)
# =========================
def kb_main(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    rows.append([InlineKeyboardButton(text="🆔 Узнать ID", callback_data="dm:myid")])

    if is_admin(user_id):
        rows.append([
            InlineKeyboardButton(text="✅ Разрешения", callback_data="dm:perm"),
            InlineKeyboardButton(text="🧾 Логи", callback_data="dm:logs"),
        ])
        rows.append([InlineKeyboardButton(text="📣 Рассылка", callback_data="dm:broadcast")])

    rows.append([InlineKeyboardButton(text="💎 VIP подписка", callback_data="dm:vip")])
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


# =========================
# HELP (подсказки)
# =========================
HELP_TEXT = (
    "📌 Команды в группе (бот реагирует только на ADMIN_IDS):\n\n"
    "<b>Разрешения рекламы</b>\n"
    "• /adgive <id|@user> [15m|2h|1d|1w|1y]\n"
    "• /adremove <id|@user>\n"
    "Можно reply: <code>/adgive 1d</code>\n\n"
    "<b>Наказания</b>\n"
    "• /mcwarn <id|@user> [причина]\n"
    "• /mcmute <id|@user> [время] [причина]\n"
    "• /mcban  <id|@user> [время] [причина]\n"
    "• /mckick <id|@user> [причина]\n"
    "• /mcunwarn /mcunmute /mcunban\n\n"
    "• /mclist [страница]\n"
    "• /chatid\n"
)

# =========================
# BOT / DP  (aiogram 3.7+)
# =========================
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


# =========================
# TOUCH (сохраняем чаты и юзеров)
# =========================
def touch(msg: Message):
    if msg.from_user:
        cache_user(msg.from_user.id, msg.from_user.username, msg.from_user.full_name)
    if msg.chat and msg.chat.type in ("group", "supergroup"):
        upsert_chat(msg.chat.id, msg.chat.title or str(msg.chat.id))


# =========================
# ГРУППА: /chatid (диагностика)
# =========================
@dp.message(Command("chatid"))
async def cmd_chatid(msg: Message):
    touch(msg)
    if msg.chat.type in ("group", "supergroup"):
        await msg.reply(f"chat_id: <code>{msg.chat.id}</code>\nтип: <b>{msg.chat.type}</b>")

@dp.message(Command("mchelp"))
async def cmd_mchelp(msg: Message):
    touch(msg)
    await msg.reply(HELP_TEXT)


# =========================
# ЛС: /start + меню
# =========================
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    touch(msg)
    if msg.chat.type != "private":
        return await msg.reply("✅ Я в чате. Команды: /mchelp, /mclist, /chatid")
    await state.clear()
    await msg.answer("🏠 Главное меню", reply_markup=kb_main(msg.from_user.id))

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


# =========================
# ЛС: разрешения (админы)
# =========================
@dp.callback_query(F.data == "dm:perm")
async def dm_perm(c: CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        return await c.answer("Нет доступа", show_alert=True)
    await state.clear()
    await c.message.edit_text(
        "✅ Разрешения на рекламу\n\n"
        "Выдача/снятие для выбранного чата.\n"
        "Можно указать: ID / @username (если бот видел) / переслать сообщение (если не скрыт автор).",
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

    await state.update_data(perm_chat_id=chat_id)
    await state.set_state(DmState.perm_wait_target)

    if action == "give":
        txt = (
            "➕ Выдать разрешение\n\n"
            "Отправьте ID / @username / перешлите сообщение пользователя.\n"
            "После этого я попрошу срок (15m/2h/1d/1w/1y) или 0 = навсегда."
        )
    else:
        txt = (
            "➖ Забрать разрешение\n\n"
            "Отправьте ID / @username / перешлите сообщение пользователя."
        )

    await c.message.edit_text(txt, reply_markup=kb_back("dm:perm"))
    await c.answer()

def extract_forward_id(msg: Message) -> int | None:
    if msg.forward_from:
        return msg.forward_from.id
    return None

async def dm_resolve_target_from_input(msg: Message) -> tuple[int | None, str | None]:
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

    uid, uname = await dm_resolve_target_from_input(msg)
    if uid is None:
        if uname and uname.startswith("@"):
            return await msg.answer(
                "❌ Я не знаю ID этого @username (бот его ещё не видел).\n\n"
                "Как исправить:\n"
                "1) Пусть пользователь напишет что-то в группе (или боту /start)\n"
                "2) Или пришли ID числом\n"
                "3) Или перешли сообщение пользователя (если автор не скрыт)",
                reply_markup=kb_back("dm:perm")
            )
        return await msg.answer(
            "❌ Не смог определить пользователя.\nПришли ID / @username / пересылку.",
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

    # give -> ждём срок
    await state.set_state(DmState.perm_wait_action)
    await msg.answer(
        f"Пользователь: <code>{uid}</code>\n"
        f"Введите срок (15m/2h/1d/1w/1y) или <b>0</b> = навсегда:",
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
# ЛС: логи рекламы (админы)
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
# ГРУППА: выдача/снятие разрешений командами (только админы)
# =========================
async def resolve_target(msg: Message, maybe: str | None) -> int | None:
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

    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("ℹ️ В ЛС выдавай через меню: ✅ Разрешения")

    parts = (msg.text or "").split()
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
            "❌ Не могу определить пользователя.\n"
            "Пример: /adgive 123456789 1d\n"
            "или reply + /adgive 1d\n"
            "(@username работает только если бот его видел)"
        )

    sec = parse_duration(dur)
    until_ts = None if sec is None else now_ts() + sec
    permit_set(msg.chat.id, uid, until_ts)

    await msg.reply(f"✅ Разрешение выдано: <code>{uid}</code>\nДо: <b>{ts_to_local_str(until_ts)}</b> {active_tag(until_ts)}")

@dp.message(Command("adremove"))
async def cmd_adremove(msg: Message):
    touch(msg)
    if not is_admin(msg.from_user.id):
        return

    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("ℹ️ В ЛС снимай через меню: ✅ Разрешения")

    parts = (msg.text or "").split()
    target = parts[1] if len(parts) >= 2 else None

    uid = await resolve_target(msg, target)
    if uid is None:
        return await msg.reply("❌ Пример: /adremove 123456789 или reply + /adremove")

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

    # НЕТ разрешения
    if not has_permit:
        try:
            await msg.delete()
        except Exception:
            pass

        # если человек написал #реклама без разрешения -> только удаляем и пишем "нет разрешения"
        if has_hashtag_anywhere(text):
            await bot.send_message(
                chat_id,
                f"🚫 {user_click_html(uid, user.full_name, user.username)}\n"
                f"Ваше сообщение удалено: <b>нет разрешения на рекламу</b>.\n"
                f"Получить разрешение: {PERMIT_HELP}"
            )
            log_ad_delete(chat_id, chat_title, uid, user.username, user.full_name, text, f"{reason} + был #реклама без разрешения")
            return

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

    # всё ок
    ad_last_set(chat_id, uid, now_ts())


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def group_any_message(msg: Message):
    touch(msg)
    if not (msg.text or msg.caption):
        return
    await auto_ad_handle(msg)


# =========================
# MC-команды (админы) + /mclist
# =========================
def reason_from(parts: list[str]) -> str:
    r = " ".join(parts).strip()
    return r if r else "не указана"

def time_and_reason(parts: list[str]) -> tuple[int | None, str]:
    if parts:
        sec = parse_duration(parts[0])
        if sec is not None:
            return sec, reason_from(parts[1:])
    return None, reason_from(parts)

async def resolve_target_cmd(msg: Message, parts: list[str]) -> tuple[int | None, list[str]]:
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = msg.reply_to_message.from_user.id
        if parts and (parts[0].isdigit() or parts[0].startswith("@")):
            if parts[0].isdigit():
                return int(parts[0]), parts[1:]
            if parts[0].startswith("@"):
                rid = resolve_username_to_id(parts[0])
                if rid is not None:
                    return rid, parts[1:]
        return target_id, parts

    if not parts:
        return None, []
    if parts[0].isdigit():
        return int(parts[0]), parts[1:]
    if parts[0].startswith("@"):
        rid = resolve_username_to_id(parts[0])
        if rid is None:
            return None, parts
        return rid, parts[1:]
    return None, parts


@dp.message(Command("mclist"))
async def cmd_mclist(msg: Message):
    touch(msg)
    if msg.chat.type not in ("group", "supergroup"):
        return

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

    await msg.reply(text)


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
        return await msg.reply("❌ Пример: /mcwarn 123456789 причина или reply + /mcwarn причина")

    reason = reason_from(rest)
    warns = mc_warn_get(msg.chat.id, target_id) + 1

    uname, full = get_user_cached(target_id)
    click = user_click_html(target_id, full, uname)

    if warns >= 4:
        mc_warn_reset(msg.chat.id, target_id)
        try:
            until_ts = await ban(msg.chat.id, target_id, MCWARN_BAN_SECONDS)
        except Exception:
            return await msg.reply("❌ Не смог выдать бан. Проверь права бота: «Блокировать пользователей».")

        mc_log(msg.chat.id, target_id, "BAN", until_ts, f"авто-бан по предупреждениям (4/3). {reason}", msg.from_user.id)

        await msg.reply(
            f"⛔ {click}\n"
            f"Счётчик предупреждений: <b>4/3</b>\n"
            f"<b>Авто-бан на 3 дня</b>\n"
            f"До: <b>{ts_to_local_str(until_ts)}</b> {active_tag(until_ts)}\n"
            f"Причина: {html_escape(reason)}"
        )
        return

    mc_warn_set(msg.chat.id, target_id, warns)
    mc_log(msg.chat.id, target_id, "WARN", None, reason, msg.from_user.id)

    await msg.reply(
        f"⚠️ {click}\n"
        f"Выдано предупреждение. Счётчик: <b>{warns}/3</b>\n"
        f"Причина: {html_escape(reason)}"
    )


# =========================
# ✅ DEBUG: ловим любые сообщения группы и пишем в Render Logs
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def debug_all_groups(msg: Message):
    # важно: этот лог должен появляться, если бот ПОЛУЧАЕТ апдейты из чата
    print("DEBUG CHAT:", msg.chat.id, msg.chat.type, (msg.text or msg.caption or "")[:200])


# =========================
# MAIN
# =========================
async def main():
    db().close()
    await start_web_server()
    print("[bot] starting polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
