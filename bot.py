import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatPermissions
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

import os
print("ENV TOKEN present:", "TOKEN" in os.environ)
TOKEN = os.environ.get("TOKEN")
print("ENV TOKEN length:", 0 if TOKEN is None else len(TOKEN))
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN.")
# =========================
# НАСТРОЙКИ (Render: Environment Variables)
# =========================
TOKEN = os.getenv("8563240122:AAGVS5stAmmC3LuhfueDeUar8nLarIForAw")  # <-- Render/Windows ENV
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN.")

ADMIN_IDS = {6911558950, 8085895186}  # доступ к ЛС меню и командам управления
DB_PATH = "mc_bot.db"
HASHTAG = "#реклама"

MUTE_2_SECONDS = 3 * 60 * 60     # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60    # 12 часов

PAGE_SIZE = 10


# =========================
# УТИЛИТЫ
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ts() -> int:
    return int(now_utc().timestamp())


def fmt_dt(ts: int | None) -> str:
    if ts is None:
        return "навсегда"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
    return dt.strftime("%d.%m.%Y %H:%M")


def active_tag(ts: int | None) -> str:
    if ts is None:
        return "[Активно]"
    return "[Активно]" if ts > now_ts() else "[Неактивно]"


def parse_duration(token: str | None) -> int | None:
    """
    '30m' '3h' '2d' -> seconds
    None/'' -> None
    """
    if not token:
        return None
    token = token.strip().lower()
    m = re.fullmatch(r"(\d{1,6})(m|h|d)", token)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    mult = {"m": 60, "h": 3600, "d": 86400}[unit]
    return n * mult


def normalize_text(t: str) -> str:
    return (t or "").strip()


def safe_cut(s: str, limit: int = 800) -> str:
    s = s or ""
    return s if len(s) <= limit else s[:limit] + "…"


# =========================
# РЕКЛАМА: КРИТЕРИИ
# =========================
KW_PATTERNS = [
    r"\bсдам\b",
    r"\bпродам\b",
    r"\bкуплю\b",
    r"\bпрайс\b",
    r"\bподпиш(итесь|ись)\b",
    r"\bподписывай(тесь|ся)\b",
]
KW_RE = re.compile("|".join(KW_PATTERNS), re.IGNORECASE)
TG_LINK_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/[A-Za-z0-9_]{3,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?<!\w)(\+?\d[\d\-\s\(\)]{8,}\d)(?!\w)")


def hashtag_at_end(text: str) -> bool:
    return bool(re.search(r"#реклама\s*$", (text or "").lower()))


def detect_ad_reason(text: str) -> tuple[bool, str, str]:
    """
    returns: (is_ad, reason_type, reason_value)
    reason_type: 'ссылка' | 'ключевое слово' | 'номер телефона'
    """
    t = text or ""

    m = TG_LINK_RE.search(t)
    if m:
        return True, "ссылка", m.group(0)

    m = KW_RE.search(t)
    if m:
        return True, "ключевое слово", m.group(0)

    m = PHONE_RE.search(t)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if len(digits) >= 10:
            return True, "номер телефона", m.group(1)

    return False, "", ""


# =========================
# БАЗА
# =========================
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            updated_at INTEGER NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS permits (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            expires_at INTEGER, -- NULL = навсегда
            issued_by INTEGER NOT NULL,
            issued_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ad_strikes (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS delete_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            message_text TEXT,
            reason_type TEXT NOT NULL,
            reason_value TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mc_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            target_username TEXT,
            target_full_name TEXT,
            action_type TEXT NOT NULL,   -- WARN/MUTE/BAN/KICK/UNWARN/UNMUTE/UNBAN
            issued_by INTEGER NOT NULL,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER,          -- для mute/ban, NULL=навсегда
            reason TEXT
        )
    """)
    con.commit()
    return con


def upsert_chat(chat_id: int, title: str | None):
    con = db()
    con.execute(
        "INSERT OR REPLACE INTO chats(chat_id, title, updated_at) VALUES (?,?,?)",
        (chat_id, title or f"Чат {chat_id}", now_ts())
    )
    con.commit()
    con.close()


def list_known_chats() -> list[tuple[int, str]]:
    con = db()
    rows = con.execute(
        "SELECT chat_id, COALESCE(title, 'Чат ' || chat_id) FROM chats ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return [(int(r[0]), str(r[1])) for r in rows]


def permit_active(chat_id: int, user_id: int) -> tuple[bool, int | None]:
    con = db()
    row = con.execute(
        "SELECT expires_at FROM permits WHERE chat_id=? AND user_id=?",
        (chat_id, user_id)
    ).fetchone()
    con.close()
    if not row:
        return False, None
    exp = row[0]
    if exp is None:
        return True, None
    return (exp > now_ts()), exp


def permit_set(chat_id: int, user_id: int, username: str | None, full_name: str | None,
               duration_seconds: int | None, issued_by: int):
    con = db()
    issued_at = now_ts()
    exp = None if duration_seconds is None else issued_at + duration_seconds
    con.execute(
        """INSERT OR REPLACE INTO permits(chat_id, user_id, username, full_name, expires_at, issued_by, issued_at)
           VALUES (?,?,?,?,?,?,?)""",
        (chat_id, user_id, (username or "").lower() if username else None, full_name, exp, issued_by, issued_at)
    )
    con.commit()
    con.close()


def permit_remove(chat_id: int, user_id: int) -> bool:
    con = db()
    cur = con.execute("DELETE FROM permits WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    ok = cur.rowcount > 0
    con.close()
    return ok


def list_permits_all(chat_id: int, page: int, page_size: int = PAGE_SIZE):
    offset = (page - 1) * page_size
    con = db()
    rows = con.execute(
        f"""
        SELECT user_id, username, full_name, expires_at, issued_at
        FROM permits
        WHERE chat_id=?
        ORDER BY
          CASE
            WHEN expires_at IS NULL THEN 1
            WHEN expires_at > ? THEN 1
            ELSE 0
          END DESC,
          issued_at DESC
        LIMIT ? OFFSET ?
        """,
        (chat_id, now_ts(), page_size, offset)
    ).fetchall()

    total = con.execute("SELECT COUNT(1) FROM permits WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, total


def strike_inc(chat_id: int, user_id: int) -> int:
    con = db()
    ts = now_ts()
    row = con.execute(
        "SELECT count FROM ad_strikes WHERE chat_id=? AND user_id=?",
        (chat_id, user_id)
    ).fetchone()
    if not row:
        con.execute(
            "INSERT INTO ad_strikes(chat_id, user_id, count, updated_at) VALUES (?,?,1,?)",
            (chat_id, user_id, ts)
        )
        con.commit()
        con.close()
        return 1
    new = int(row[0]) + 1
    con.execute(
        "UPDATE ad_strikes SET count=?, updated_at=? WHERE chat_id=? AND user_id=?",
        (new, ts, chat_id, user_id)
    )
    con.commit()
    con.close()
    return new


def strike_reset(chat_id: int, user_id: int):
    con = db()
    con.execute("DELETE FROM ad_strikes WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    con.commit()
    con.close()


def log_deletion(chat_id: int, user, text: str, reason_type: str, reason_value: str):
    con = db()
    con.execute(
        """INSERT INTO delete_logs(chat_id, user_id, username, full_name, message_text, reason_type, reason_value, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            chat_id,
            user.id,
            (user.username or "").lower() if user.username else None,
            user.full_name,
            (text or "")[:800],
            reason_type,
            reason_value[:200],
            now_ts()
        )
    )
    con.commit()
    con.close()


def add_mc_action(chat_id: int, target_user_id: int, target_username: str | None, target_full_name: str,
                  action_type: str, issued_by: int, expires_at: int | None, reason: str | None):
    con = db()
    con.execute(
        """INSERT INTO mc_actions(chat_id, target_user_id, target_username, target_full_name, action_type,
                                  issued_by, issued_at, expires_at, reason)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            chat_id,
            target_user_id,
            (target_username or "").lower() if target_username else None,
            target_full_name,
            action_type,
            issued_by,
            now_ts(),
            expires_at,
            reason or "не указана"
        )
    )
    con.commit()
    con.close()


def list_mc_actions(chat_id: int, page: int, page_size: int = PAGE_SIZE):
    offset = (page - 1) * page_size
    con = db()
    rows = con.execute(
        """SELECT target_user_id, target_username, target_full_name, action_type, issued_at, expires_at, reason
           FROM mc_actions
           WHERE chat_id=?
           ORDER BY id DESC
           LIMIT ? OFFSET ?""",
        (chat_id, page_size, offset)
    ).fetchall()
    total = con.execute("SELECT COUNT(1) FROM mc_actions WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, total


def list_log_chats():
    con = db()
    rows = con.execute(
        """SELECT DISTINCT dl.chat_id, COALESCE(c.title, 'Чат ' || dl.chat_id)
           FROM delete_logs dl
           LEFT JOIN chats c ON c.chat_id = dl.chat_id
           ORDER BY dl.chat_id DESC"""
    ).fetchall()
    con.close()
    return [(int(r[0]), str(r[1])) for r in rows]


def list_delete_logs(chat_id: int, page: int, page_size: int = PAGE_SIZE):
    offset = (page - 1) * page_size
    con = db()
    rows = con.execute(
        """SELECT user_id, username, full_name, message_text, reason_type, reason_value, created_at
           FROM delete_logs
           WHERE chat_id=?
           ORDER BY id DESC
           LIMIT ? OFFSET ?""",
        (chat_id, page_size, offset)
    ).fetchall()
    total = con.execute("SELECT COUNT(1) FROM delete_logs WHERE chat_id=?", (chat_id,)).fetchone()[0]
    con.close()
    return rows, total


# =========================
# FSM (ЛС) — Разрешения
# =========================
class DmPermFSM(StatesGroup):
    waiting_user_for_grant = State()
    waiting_user_for_remove = State()


# =========================
# КЛАВИАТУРЫ (ЛС)
# =========================
def kb_dm_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="dm:profile"),
            InlineKeyboardButton(text="✅ Разрешения", callback_data="dm:perm"),
        ],
        [InlineKeyboardButton(text="💎 VIP подписка", callback_data="dm:vip")],
        [InlineKeyboardButton(text="📜 Логи", callback_data="dm:logs")],
    ])


def kb_dm_back(cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=cb)]
    ])


def kb_dm_profile() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="dm:stats:chats")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="dm:home")],
    ])


def kb_dm_perm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать", callback_data="dm:perm:give:chats")],
        [InlineKeyboardButton(text="➖ Забрать", callback_data="dm:perm:remove:chats")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="dm:home")],
    ])


def kb_dm_chats(prefix: str, chats: list[tuple[int, str]], back_cb: str) -> InlineKeyboardMarkup:
    kb = []
    for chat_id, title in chats[:25]:
        kb.append([InlineKeyboardButton(text=title, callback_data=f"{prefix}:{chat_id}")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def kb_dm_logs_chats(chats: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    kb = []
    for chat_id, title in chats[:25]:
        kb.append([InlineKeyboardButton(text=title, callback_data=f"dm:logchat:{chat_id}:1")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def kb_duration_picker(chat_id: int, user_id: int, back_key: str) -> InlineKeyboardMarkup:
    # back_key: 'perm' or 'home'
    btns = [
        ("15m", 15 * 60),
        ("1h", 60 * 60),
        ("3h", 3 * 60 * 60),
        ("1d", 24 * 60 * 60),
        ("7d", 7 * 24 * 60 * 60),
        ("∞", 0),
    ]
    rows = [
        [
            InlineKeyboardButton(text=btns[0][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[0][1]}:{back_key}"),
            InlineKeyboardButton(text=btns[1][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[1][1]}:{back_key}"),
            InlineKeyboardButton(text=btns[2][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[2][1]}:{back_key}"),
        ],
        [
            InlineKeyboardButton(text=btns[3][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[3][1]}:{back_key}"),
            InlineKeyboardButton(text=btns[4][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[4][1]}:{back_key}"),
            InlineKeyboardButton(text=btns[5][0], callback_data=f"dm:dur2:{chat_id}:{user_id}:{btns[5][1]}:{back_key}"),
        ],
    ]
    back_cb = "dm:perm" if back_key == "perm" else "dm:home"
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_stats_pager(chat_id: int, page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    if has_prev:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"dm:stats:chat:{chat_id}:{page-1}"))
    if has_next:
        row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"dm:stats:chat:{chat_id}:{page+1}"))

    kb = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton(text="⬅️ К чатам", callback_data="dm:stats:chats")])
    kb.append([InlineKeyboardButton(text="⬅️ Профиль", callback_data="dm:profile")])
    kb.append([InlineKeyboardButton(text="🏠 Меню", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def kb_logs_pager(chat_id: int, page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    if has_prev:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"dm:logchat:{chat_id}:{page-1}"))
    if has_next:
        row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"dm:logchat:{chat_id}:{page+1}"))
    kb = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton(text="⬅️ К чатам", callback_data="dm:logs")])
    kb.append([InlineKeyboardButton(text="🏠 Меню", callback_data="dm:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


# =========================
# КЛАВИАТУРА /mclist
# =========================
def kb_mclist_pager(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    if has_prev:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"mclist:{page-1}"))
    if has_next:
        row.append(InlineKeyboardButton(text="Дальше ➡️", callback_data=f"mclist:{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[row]) if row else InlineKeyboardMarkup(inline_keyboard=[])


# =========================
# БОТ
# =========================
bot = Bot(TOKEN)
dp = Dispatcher()


# =========================
# /start
# =========================
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await state.clear()
    if msg.chat.type == "private":
        if not is_admin(msg.from_user.id):
            return await msg.answer("❌ Нет доступа.")
        await msg.answer("Меню:", reply_markup=kb_dm_main())
    else:
        await msg.reply("✅ Я в чате. Используй /mclist для списка наказаний.")


# =========================
# ЛС: Меню
# =========================
@dp.callback_query(F.data == "dm:home")
async def dm_home(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)
    await state.clear()
    await cq.message.edit_text("Меню:", reply_markup=kb_dm_main())
    await cq.answer()


@dp.callback_query(F.data == "dm:profile")
async def dm_profile(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    status = "Админ" if is_admin(cq.from_user.id) else "Участник"
    text = (
        "👤 Профиль\n\n"
        f"Статус: {status}\n"
        f"ID: {cq.from_user.id}\n"
    )
    await cq.message.edit_text(text, reply_markup=kb_dm_profile())
    await cq.answer()


@dp.callback_query(F.data == "dm:perm")
async def dm_perm(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)
    await state.clear()
    await cq.message.edit_text("✅ Разрешения\nВыберите действие:", reply_markup=kb_dm_perm())
    await cq.answer()


@dp.callback_query(F.data == "dm:vip")
async def dm_vip(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    await cq.message.edit_text(
        "💎 VIP подписка\n\n"
        "Пока заглушка.\n"
        "Идеи:\n"
        "• расширенные лимиты рекламы\n"
        "• мягче фильтр\n"
        "• бейдж\n",
        reply_markup=kb_dm_back("dm:home")
    )
    await cq.answer()


# =========================
# ЛС: Логи удалений
# =========================
@dp.callback_query(F.data == "dm:logs")
async def dm_logs_choose_chat(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    chats = list_log_chats()
    if not chats:
        await cq.message.edit_text("Логов пока нет.", reply_markup=kb_dm_back("dm:home"))
        await cq.answer()
        return

    await cq.message.edit_text("📜 Логи — выберите чат:", reply_markup=kb_dm_logs_chats(chats))
    await cq.answer()


@dp.callback_query(F.data.startswith("dm:logchat:"))
async def dm_logs_show(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    parts = cq.data.split(":")
    chat_id = int(parts[2])
    page = int(parts[3])

    rows, total = list_delete_logs(chat_id, page, PAGE_SIZE)
    if total == 0:
        await cq.message.edit_text("Логов пока нет.", reply_markup=kb_dm_back("dm:logs"))
        await cq.answer()
        return

    lines = [f"📜 Логи удалений — чат {chat_id} (стр. {page})\n"]
    for (uid, uname, full_name, text, rtype, rval, created_at) in rows:
        dt = datetime.fromtimestamp(created_at, tz=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
        who = f"@{uname}" if uname else full_name
        lines.append(
            "🗑 Сообщение удалено\n"
            f"👤 Пользователь: {who}\n"
            f"🆔 ID: {uid}\n"
            f"📅 {dt}\n"
            f"📌 Причина: {rtype}: {rval}\n"
            f"💬 Текст: {safe_cut(text, 600)}\n"
            "— — —"
        )

    has_prev = page > 1
    has_next = total > page * PAGE_SIZE
    await cq.message.edit_text(
        "\n".join(lines)[:3900],
        reply_markup=kb_logs_pager(chat_id, page, has_prev, has_next)
    )
    await cq.answer()


# =========================
# ЛС: Профиль -> Статистика
# =========================
@dp.callback_query(F.data == "dm:stats:chats")
async def dm_stats_chats(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    chats = list_known_chats()
    if not chats:
        await cq.message.edit_text("Пока нет данных о чатах.", reply_markup=kb_dm_back("dm:profile"))
        await cq.answer()
        return

    kb = []
    for chat_id, title in chats[:25]:
        kb.append([InlineKeyboardButton(text=title, callback_data=f"dm:stats:chat:{chat_id}:1")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="dm:profile")])

    await cq.message.edit_text("📊 Статистика — выберите чат:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await cq.answer()


@dp.callback_query(F.data.startswith("dm:stats:chat:"))
async def dm_stats_chat_page(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    parts = cq.data.split(":")
    chat_id = int(parts[3])
    page = int(parts[4])

    rows, total = list_permits_all(chat_id, page, PAGE_SIZE)
    if total == 0:
        await cq.message.edit_text(
            f"📊 Статистика чата {chat_id}\n\nПока нет выданных разрешений.",
            reply_markup=kb_dm_back("dm:stats:chats")
        )
        await cq.answer()
        return

    lines = [f"📊 Статистика чата {chat_id} (стр. {page})\n"]
    for (uid, uname, full_name, expires_at, _issued_at) in rows:
        who = f"@{uname}" if uname else (full_name or str(uid))
        lines.append(
            f"{who} | {uid}\n"
            f"Разрешение до: {fmt_dt(expires_at)} {active_tag(expires_at)}\n"
            "— — —"
        )

    has_prev = page > 1
    has_next = total > page * PAGE_SIZE
    await cq.message.edit_text(
        "\n".join(lines)[:3900],
        reply_markup=kb_stats_pager(chat_id, page, has_prev, has_next)
    )
    await cq.answer()


# =========================
# ЛС: Разрешения (выдать/забрать)
# =========================
@dp.callback_query(F.data == "dm:perm:give:chats")
async def dm_perm_give_choose_chat(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)
    await state.clear()

    chats = list_known_chats()
    if not chats:
        await cq.message.edit_text("Пока нет данных о чатах.", reply_markup=kb_dm_back("dm:perm"))
        await cq.answer()
        return

    await cq.message.edit_text(
        "➕ Выдать разрешение — выберите чат:",
        reply_markup=kb_dm_chats("dm:perm:givechat", chats, "dm:perm")
    )
    await cq.answer()


@dp.callback_query(F.data == "dm:perm:remove:chats")
async def dm_perm_remove_choose_chat(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)
    await state.clear()

    chats = list_known_chats()
    if not chats:
        await cq.message.edit_text("Пока нет данных о чатах.", reply_markup=kb_dm_back("dm:perm"))
        await cq.answer()
        return

    await cq.message.edit_text(
        "➖ Забрать разрешение — выберите чат:",
        reply_markup=kb_dm_chats("dm:perm:removechat", chats, "dm:perm")
    )
    await cq.answer()


@dp.callback_query(F.data.startswith("dm:perm:givechat:"))
async def dm_perm_give_chat_selected(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    chat_id = int(cq.data.split(":")[3])
    await state.update_data(target_chat_id=chat_id)
    await state.set_state(DmPermFSM.waiting_user_for_grant)

    await cq.message.edit_text(
        "Отправьте ID пользователя (число) ИЛИ перешлите сообщение пользователя.\n\n"
        "Пример: `123456789`",
        reply_markup=kb_dm_back("dm:perm"),
        parse_mode="Markdown"
    )
    await cq.answer()


@dp.callback_query(F.data.startswith("dm:perm:removechat:"))
async def dm_perm_remove_chat_selected(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    chat_id = int(cq.data.split(":")[3])
    await state.update_data(target_chat_id=chat_id)
    await state.set_state(DmPermFSM.waiting_user_for_remove)

    await cq.message.edit_text(
        "Отправьте ID пользователя (число) ИЛИ перешлите сообщение пользователя.",
        reply_markup=kb_dm_back("dm:perm")
    )
    await cq.answer()


def extract_user_id_from_forward(msg: Message) -> int | None:
    if msg.forward_from:
        return msg.forward_from.id
    if msg.reply_to_message and msg.reply_to_message.forward_from:
        return msg.reply_to_message.forward_from.id
    return None


@dp.message(DmPermFSM.waiting_user_for_grant)
async def dm_perm_wait_user_grant(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    chat_id = int(data.get("target_chat_id", 0))
    if chat_id == 0:
        await state.clear()
        return await msg.answer("Ошибка: чат не выбран.", reply_markup=kb_dm_main())

    user_id = None
    if (msg.text or "").strip().isdigit():
        user_id = int((msg.text or "").strip())
    else:
        user_id = extract_user_id_from_forward(msg)

    if user_id is None:
        return await msg.answer("Не смог определить пользователя. Пришлите ID (число) или перешлите сообщение с автором.")

    await state.clear()
    await msg.answer(
        f"Пользователь ID: {user_id}\nВыберите срок:",
        reply_markup=kb_duration_picker(chat_id, user_id, "perm")
    )


@dp.message(DmPermFSM.waiting_user_for_remove)
async def dm_perm_wait_user_remove(msg: Message, state: FSMContext):
    if msg.chat.type != "private":
        return
    if not is_admin(msg.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    chat_id = int(data.get("target_chat_id", 0))
    if chat_id == 0:
        await state.clear()
        return await msg.answer("Ошибка: чат не выбран.", reply_markup=kb_dm_main())

    user_id = None
    if (msg.text or "").strip().isdigit():
        user_id = int((msg.text or "").strip())
    else:
        user_id = extract_user_id_from_forward(msg)

    if user_id is None:
        return await msg.answer("Не смог определить пользователя. Пришлите ID (число) или перешлите сообщение с автором.")

    ok = permit_remove(chat_id, user_id)
    await state.clear()
    await msg.answer(
        "✅ Успешно выполнено. Можете вернуться в меню." if ok else "ℹ️ Разрешения не было.",
        reply_markup=kb_dm_main()
    )


@dp.callback_query(F.data.startswith("dm:dur2:"))
async def dm_duration_selected(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Нет доступа", show_alert=True)

    # dm:dur2:<chat_id>:<user_id>:<seconds>:<back_key>
    parts = cq.data.split(":")
    chat_id = int(parts[2])
    user_id = int(parts[3])
    seconds = int(parts[4])
    back_key = parts[5]

    duration = None if seconds == 0 else seconds
    permit_set(chat_id, user_id, None, None, duration, cq.from_user.id)

    _, exp = permit_active(chat_id, user_id)
    back_cb = "dm:perm" if back_key == "perm" else "dm:home"

    await cq.message.edit_text(
        f"✅ Успешно выполнено.\n\n"
        f"Чат: {chat_id}\n"
        f"Пользователь ID: {user_id}\n"
        f"До: {fmt_dt(exp)} {active_tag(exp)}\n\n"
        f"Можете вернуться в меню.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)],
            [InlineKeyboardButton(text="🏠 Меню", callback_data="dm:home")]
        ])
    )
    await cq.answer()


# =========================
# /mclist (чат) + пагинация
# =========================
@dp.message(Command("mclist"))
async def mclist_cmd(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return await msg.reply("Команда доступна только в группе.")

    parts = (msg.text or "").split()
    page = 1
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))

    rows, total = list_mc_actions(msg.chat.id, page, PAGE_SIZE)
    if total == 0:
        return await msg.reply("Список наказаний пуст.")

    lines = [f"📋 /mclist — последние наказания (стр. {page})\n"]
    for (uid, uname, full_name, atype, issued_at, expires_at, reason) in rows:
        who = f"@{uname}" if uname else (full_name or str(uid))
        issued = datetime.fromtimestamp(issued_at, tz=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
        end = fmt_dt(expires_at)
        status = active_tag(expires_at) if atype in ("MUTE", "BAN") else ""
        lines.append(
            f"{who} | {atype}\n"
            f"Выдано: {issued}\n"
            f"До: {end} {status}\n"
            f"Причина: {reason}\n"
            "— — —"
        )

    has_prev = page > 1
    has_next = total > page * PAGE_SIZE
    await msg.reply("\n".join(lines)[:3900], reply_markup=kb_mclist_pager(page, has_prev, has_next))


@dp.callback_query(F.data.startswith("mclist:"))
async def mclist_pager_cb(cq: CallbackQuery):
    if cq.message.chat.type not in ("group", "supergroup"):
        return await cq.answer()
    page = int(cq.data.split(":")[1])

    rows, total = list_mc_actions(cq.message.chat.id, page, PAGE_SIZE)
    if total == 0:
        await cq.message.edit_text("Список наказаний пуст.")
        await cq.answer()
        return

    lines = [f"📋 /mclist — последние наказания (стр. {page})\n"]
    for (uid, uname, full_name, atype, issued_at, expires_at, reason) in rows:
        who = f"@{uname}" if uname else (full_name or str(uid))
        issued = datetime.fromtimestamp(issued_at, tz=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")
        end = fmt_dt(expires_at)
        status = active_tag(expires_at) if atype in ("MUTE", "BAN") else ""
        lines.append(
            f"{who} | {atype}\n"
            f"Выдано: {issued}\n"
            f"До: {end} {status}\n"
            f"Причина: {reason}\n"
            "— — —"
        )

    has_prev = page > 1
    has_next = total > page * PAGE_SIZE
    await cq.message.edit_text("\n".join(lines)[:3900], reply_markup=kb_mclist_pager(page, has_prev, has_next))
    await cq.answer()


# =========================
# ВСПОМОГАТЕЛЬНОЕ: цель наказания (reply или ID)
# =========================
async def resolve_target(msg: Message, args: list[str]) -> tuple[int | None, str | None, str | None, list[str]]:
    reply_user = msg.reply_to_message.from_user if msg.reply_to_message else None

    def looks_duration(x: str) -> bool:
        return parse_duration(x) is not None

    if reply_user:
        if not args:
            return reply_user.id, reply_user.username, reply_user.full_name, []
        if looks_duration(args[0]):
            return reply_user.id, reply_user.username, reply_user.full_name, args
        if args[0].isdigit():
            return int(args[0]), None, "Пользователь", args[1:]
        if args[0].startswith("@"):
            target_un = args[0].lstrip("@").lower()
            if reply_user.username and reply_user.username.lower() == target_un:
                return reply_user.id, reply_user.username, reply_user.full_name, args[1:]
            return None, target_un, None, args[1:]
        return reply_user.id, reply_user.username, reply_user.full_name, args

    if not args:
        return None, None, None, []
    if args[0].isdigit():
        return int(args[0]), None, "Пользователь", args[1:]
    if args[0].startswith("@"):
        return None, args[0].lstrip("@").lower(), None, args[1:]
    return None, None, None, args


def split_reason(tokens: list[str]) -> str:
    return " ".join(tokens).strip() if tokens else "не указана"


async def ensure_group(msg: Message) -> bool:
    if msg.chat.type not in ("group", "supergroup"):
        await msg.reply("Эта команда работает только в группе.")
        return False
    return True


# =========================
# MC-команды (ручные)
# =========================
@dp.message(Command("mcwarn"))
async def mcwarn(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)
    reason = split_reason(rest)

    if target_id is None:
        return await msg.reply("Нужно reply на сообщение или указать ID: /mcwarn 123456789 причина")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "WARN", msg.from_user.id, None, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"⚠️ Выдано предупреждение: {who}\nПричина: {reason}")


@dp.message(Command("mcunwarn"))
async def mcunwarn(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)
    reason = split_reason(rest)

    if target_id is None:
        return await msg.reply("Нужно reply на сообщение или указать ID: /mcunwarn 123456789")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "UNWARN", msg.from_user.id, None, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"✅ Сброс предупреждений: {who}\nПричина: {reason}")


@dp.message(Command("mcmute"))
async def mcmute(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)

    if target_id is None:
        return await msg.reply("Нужно reply или ID. Пример: /mcmute 123456789 3h причина")

    duration = None
    if rest and parse_duration(rest[0]) is not None:
        duration = parse_duration(rest[0])
        rest = rest[1:]

    reason = split_reason(rest)

    if duration is None:
        duration = 10 * 365 * 24 * 60 * 60  # "навсегда" ~10 лет

    until = now_utc() + timedelta(seconds=duration)
    expires_at = int(until.timestamp())

    try:
        await bot.restrict_chat_member(
            chat_id=msg.chat.id,
            user_id=target_id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until
        )
    except Exception:
        return await msg.reply("❌ Не смог выдать мут. Проверь права бота: 'Ограничивать участников'.")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "MUTE", msg.from_user.id, expires_at, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"🔇 Мут выдан: {who}\nДо: {fmt_dt(expires_at)} {active_tag(expires_at)}\nПричина: {reason}")


@dp.message(Command("mcunmute"))
async def mcunmute(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)
    reason = split_reason(rest)

    if target_id is None:
        return await msg.reply("Нужно reply или ID: /mcunmute 123456789")

    try:
        await bot.restrict_chat_member(
            chat_id=msg.chat.id,
            user_id=target_id,
            permissions=ChatPermissions(can_send_messages=True)
        )
    except Exception:
        return await msg.reply("❌ Не смог снять мут. Проверь права бота.")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "UNMUTE", msg.from_user.id, None, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"✅ Мут снят: {who}\nПричина: {reason}")


@dp.message(Command("mckick"))
async def mckick(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)
    reason = split_reason(rest)

    if target_id is None:
        return await msg.reply("Нужно reply или ID: /mckick 123456789 причина")

    try:
        await bot.ban_chat_member(chat_id=msg.chat.id, user_id=target_id, until_date=now_utc() + timedelta(seconds=60))
        await bot.unban_chat_member(chat_id=msg.chat.id, user_id=target_id)
    except Exception:
        return await msg.reply("❌ Не смог кикнуть. Проверь права бота: бан/кик.")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "KICK", msg.from_user.id, None, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"👢 Кик: {who}\nПричина: {reason}")


@dp.message(Command("mcban"))
async def mcban(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)

    if target_id is None:
        return await msg.reply("Нужно reply или ID. Пример: /mcban 123456789 1d причина")

    duration = None
    if rest and parse_duration(rest[0]) is not None:
        duration = parse_duration(rest[0])
        rest = rest[1:]
    reason = split_reason(rest)

    until = None
    expires_at = None
    if duration is not None:
        until = now_utc() + timedelta(seconds=duration)
        expires_at = int(until.timestamp())

    try:
        if until is None:
            await bot.ban_chat_member(chat_id=msg.chat.id, user_id=target_id)
        else:
            await bot.ban_chat_member(chat_id=msg.chat.id, user_id=target_id, until_date=until)
    except Exception:
        return await msg.reply("❌ Не смог забанить. Проверь права бота: бан.")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "BAN", msg.from_user.id, expires_at, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"⛔ Бан: {who}\nДо: {fmt_dt(expires_at)} {active_tag(expires_at)}\nПричина: {reason}")


@dp.message(Command("mcunban"))
async def mcunban(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)
    reason = split_reason(rest)

    if target_id is None:
        return await msg.reply("Нужно reply или ID: /mcunban 123456789")

    try:
        await bot.unban_chat_member(chat_id=msg.chat.id, user_id=target_id)
    except Exception:
        return await msg.reply("❌ Не смог разбанить. Проверь права бота.")

    add_mc_action(msg.chat.id, target_id, target_uname, target_name or str(target_id),
                  "UNBAN", msg.from_user.id, None, reason)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"✅ Разбан: {who}\nПричина: {reason}")


# =========================
# Разрешения (чат) — команды
# =========================
@dp.message(Command("adgive"))
async def adgive(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, rest = await resolve_target(msg, parts)

    if target_id is None:
        return await msg.reply("Нужно reply или ID: /adgive 123456789 1d (или без времени = навсегда)")

    duration = None
    if rest and parse_duration(rest[0]) is not None:
        duration = parse_duration(rest[0])

    permit_set(msg.chat.id, target_id, target_uname, target_name, duration, msg.from_user.id)
    _, exp = permit_active(msg.chat.id, target_id)

    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply(f"✅ Разрешение на рекламу выдано: {who}\nДо: {fmt_dt(exp)} {active_tag(exp)}")


@dp.message(Command("adremove"))
async def adremove(msg: Message):
    if not await ensure_group(msg):
        return
    if not is_admin(msg.from_user.id):
        return

    parts = (msg.text or "").split()[1:]
    target_id, target_uname, target_name, _rest = await resolve_target(msg, parts)

    if target_id is None:
        return await msg.reply("Нужно reply или ID: /adremove 123456789")

    ok = permit_remove(msg.chat.id, target_id)
    who = f"@{target_uname}" if target_uname else (target_name or str(target_id))
    await msg.reply("✅ Разрешение снято: " + who if ok else "ℹ️ Разрешения не было.")


# =========================
# АВТОМОДЕРАЦИЯ РЕКЛАМЫ
# =========================
@dp.message(F.text)
async def auto_moderation(msg: Message):
    if msg.chat.type not in ("group", "supergroup"):
        return

    upsert_chat(msg.chat.id, msg.chat.title)

    text = normalize_text(msg.text)
    is_ad, rtype, rval = detect_ad_reason(text)
    if not is_ad:
        return

    perm_ok, _perm_exp = permit_active(msg.chat.id, msg.from_user.id)

    # есть разрешение -> нужен тег в конце
    if perm_ok:
        if not hashtag_at_end(text):
            try:
                await msg.delete()
            except Exception:
                pass
            log_deletion(msg.chat.id, msg.from_user, text, rtype, rval)
            await bot.send_message(
                msg.chat.id,
                'Ваше сообщение удалено, по причине отсутствия тега на рекламу.\n'
                'Пожалуйста укажите в вашей рекламе тег "#реклама".'
            )
        return

    # разрешения нет -> удаляем и наказываем
    try:
        await msg.delete()
    except Exception:
        pass
    log_deletion(msg.chat.id, msg.from_user, text, rtype, rval)

    stage = strike_inc(msg.chat.id, msg.from_user.id)

    if stage == 1:
        await bot.send_message(
            msg.chat.id,
            f"⚠️ {msg.from_user.full_name}: выдано предупреждение.\n"
            f"Причина: {rtype}: {rval}"
        )
        return

    if stage == 2:
        until = now_utc() + timedelta(seconds=MUTE_2_SECONDS)
        try:
            await bot.restrict_chat_member(
                chat_id=msg.chat.id,
                user_id=msg.from_user.id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until
            )
            exp = int(until.timestamp())
            await bot.send_message(
                msg.chat.id,
                f"🔇 {msg.from_user.full_name}: мут 3 часа.\n"
                f"До: {fmt_dt(exp)} {active_tag(exp)}\n"
                f"Причина: {rtype}: {rval}"
            )
        except Exception:
            await bot.send_message(msg.chat.id, "❌ Не могу выдать мут — дай мне право 'Ограничивать участников'.")
        return

    # 3 стадия: мут 12ч + сброс
    until = now_utc() + timedelta(seconds=MUTE_3_SECONDS)
    try:
        await bot.restrict_chat_member(
            chat_id=msg.chat.id,
            user_id=msg.from_user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until
        )
        exp = int(until.timestamp())
        await bot.send_message(
            msg.chat.id,
            f"⛔ {msg.from_user.full_name}: мут 12 часов.\n"
            f"До: {fmt_dt(exp)} {active_tag(exp)}\n"
            f"Причина: {rtype}: {rval}\n"
            f"Счётчик предупреждений сброшен."
        )
    except Exception:
        await bot.send_message(msg.chat.id, "❌ Не могу выдать мут — дай мне право 'Ограничивать участников'.")
    finally:
        strike_reset(msg.chat.id, msg.from_user.id)


# =========================
# MAIN
# =========================
async def main():
    db().close()
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
