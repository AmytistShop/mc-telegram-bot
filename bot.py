import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone

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
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN in /etc/environment")

ADMIN_IDS = {8085895186}

DB_PATH = "mc_bot.db"
HASHTAG = "#реклама"

# анти-реклама: стадии наказаний бота (без разрешения)
MUTE_2_SECONDS = 3 * 60 * 60       # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60      # 12 часов

# лимит рекламы по разрешению
ADS_COOLDOWN_SECONDS = 24 * 60 * 60

# неактивность разрешения (если не юзал месяц)
PERMIT_INACTIVE_SECONDS = 30 * 24 * 60 * 60  # 30 дней

RULES_LINK = "https://leoned777.github.io/chats/"
SUPPORT_BOT_FOR_PERMIT = "@minecrfat_bot"

# ВСТАВЬ СТИКЕР ID (по желанию). Если None — стикер не отправляется.
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
# АНТИ-РЕКЛАМА (правила)
# =========================
KW = ["продам", "куплю", "сдам", "прайс", "подпишитесь", "подписывайтесь"]
TELEGRAM_LINK = re.compile(r"(https?://)?t\.me/[\w_]{3,}", re.I)
PHONE = re.compile(r"(\+?\d[\d\-\s]{8,}\d)")

def is_ad_message(text: str | None) -> tuple[bool, str]:
    """
    Возвращает (True/False, причина)
    @username НЕ считается рекламой (мы не ищем по @ вообще).
    """
    t = (text or "").lower()
    if TELEGRAM_LINK.search(t):
        return True, "ссылка t.me"
    if PHONE.search(t):
        return True, "номер телефона"
    for w in KW:
        if w in t:
            return True, f'ключевое слово: "{w}"'
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
    """
    Возвращает пользователей, у кого разрешение НЕ истекло:
      (user_id, until_ts, last_ad_ts)
    """
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
# ГРУППА: наказания (mute/ban/kick)
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
# ГРУППА: /adgive /adrevoke
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
# ГРУППА: снятие наказаний (оставили)
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
# /mclist (оставили)
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

    lines = [f"📋 <
