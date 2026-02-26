import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from html import escape

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatPermissions,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext


# =========================
# НАСТРОЙКИ
# =========================
TOKEN = (os.getenv("TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("TOKEN is not set. Add env var TOKEN in Render (no quotes, no spaces).")

ADMIN_IDS = {8085895186}

DB_PATH = "mc_bot.db"
HASHTAG = "#реклама"

# анти-реклама: стадии наказаний
MUTE_2_SECONDS = 3 * 60 * 60       # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60      # 12 часов

# лимит рекламы по разрешению
ADS_COOLDOWN_SECONDS = 24 * 60 * 60

# админские предупреждения: 4/3 => бан 3 дня
ADMIN_WARN_LIMIT = 4
ADMIN_WARN_AUTOBAN_SECONDS = 3 * 24 * 60 * 60

RULES_LINK = "https://leoned777.github.io/chats/"
SUPPORT_BOT = "@minecraft_chat_igra_bot"

MC_LIST_PAGE_SIZE = 10

FULL_PERMS = ChatPermissions(
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
    can_change_info=True,
    can_invite_users=True,
    can_pin_messages=True,
    can_manage_topics=True,
)


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

def is_active(until_ts: int | None) -> bool:
    return True if until_ts is None else until_ts > ts()

def active_tag(until_ts: int | None) -> str:
    return "[Активно]" if is_active(until_ts) else "[Неактивно]"

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

def user_link(uid: int, name: str | None) -> str:
    safe_name = escape(name or str(uid))
    return f'<a href="tg://user?id={uid}">{safe_name}</a>'

def fmt_left(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"

def parse_cmd_parts(msg: Message) -> list[str]:
    return (msg.text or "").split()

async def get_target_from_command(msg: Message) -> int | None:
    # reply
    if msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user.id

    # @username / id
    parts = parse_cmd_parts(msg)
    if len(parts) >= 2:
        p = parts[1].strip()
        if p.isdigit():
            return int(p)
        if p.startswith("@"):
            try:
                ch = await bot.get_chat(p[1:])
                return int(ch.id)
            except Exception:
                return None
    return None

async def get_target_name_username_from_reply(msg: Message) -> tuple[str | None, str | None]:
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        return u.full_name, u.username
    return None, None


# =========================
# АНТИ-РЕКЛАМА (правила)
# =========================
KW = ["продам", "куплю", "сдам", "прайс", "подпишитесь", "подписывайтесь"]
TELEGRAM_LINK = re.compile(r"(https?://)?t\.me/[\w_]{3,}", re.I)
PHONE = re.compile(r"(\+?\d[\d\-\s\(\)]{8,}\d)")

def is_ad_message(text: str | None) -> tuple[bool, str]:
    """
    Возвращает (True/False, причина)
    @username НЕ считается рекламой.
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
    CREATE TABLE IF NOT EXISTS admin_warns (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(chat_id, user_id)
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
        kind TEXT NOT NULL,
        until_ts INTEGER,
        reason TEXT,
        issued_ts INTEGER NOT NULL,
        issued_by INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY(chat_id, user_id, kind)
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


# ----- стадии рекламы -----
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


# ----- админские предупреждения -----
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


# ----- наказания для /mclist -----
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
# КЛАВИАТУРЫ
# =========================
def kb_main(is_admin_flag: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🆔 Узнать ID", callback_data="my_id")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
    ]
    if is_admin_flag:
        rows += [
            [InlineKeyboardButton(text="✅ Разрешения", callback_data="perm_menu")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="bc_menu")],
            [InlineKeyboardButton(text="💬 Сообщения", callback_data="support_admin")],
        ]
    rows += [
        [InlineKeyboardButton(text="☎️ Связаться с админом", callback_data="support_user")],
        [InlineKeyboardButton(text="⭐ VIP подписка", callback_data="vip")],
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


# =========================
# БОТ
# =========================
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


# =========================
# /start /cancel /chatid
# =========================
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    flag = is_admin(msg.from_user.id)
    await msg.answer(
        "🏠 <b>Главное меню</b>\n\n"
        f"📌 Тег рекламы в конце: <code>{HASHTAG}</code>",
        reply_markup=kb_main(flag)
    )

@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("❌ Отменено.", reply_markup=kb_main(is_admin(msg.from_user.id)))

@dp.message(Command("chatid"))
async def cmd_chatid(msg: Message):
    if msg.chat.type in ("group", "supergroup"):
        await msg.reply(f"✅ chat_id: <code>{msg.chat.id}</code>")
    else:
        await msg.answer("ℹ️ <b>/chatid</b> работает только в группе.")


# =========================
# HELP для /userid
# =========================
HELP_FORMS_USERID = "Форма: <code>/userid</code> — используй <u>ответом</u> на сообщение пользователя (админы)."


# =========================
# /userid (только админы, только reply)
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("userid"))
async def cmd_userid(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)
    if not is_admin(msg.from_user.id):
        return
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        return await msg.reply(HELP_FORMS_USERID)
    u = msg.reply_to_message.from_user
    await msg.reply(f"🆔 {user_link(u.id, u.full_name)}: <code>{u.id}</code>")


# =========================
# /adgive /adremove (в чате, админы)
# =========================
HELP_ADGIVE = (
    "✅ Выдать разрешение рекламы\n"
    "Форма:\n"
    "• ответом: <code>/adgive 1d</code> (срок опционально)\n"
    "• или: <code>/adgive @user 1d</code>\n"
    "Если срок не указать — навсегда."
)
HELP_ADREMOVE = (
    "🗑️ Убрать разрешение рекламы\n"
    "Форма:\n"
    "• ответом: <code>/adremove</code>\n"
    "• или: <code>/adremove @user</code>"
)

@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("adgive"))
async def cmd_adgive(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)
    if not is_admin(msg.from_user.id):
        return

    parts = parse_cmd_parts(msg)

    # reply: /adgive 1d
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = msg.reply_to_message.from_user.id
        target_name = msg.reply_to_message.from_user.full_name
        dur_token = parts[1] if len(parts) >= 2 else None
    else:
        # /adgive @user 1d
        if len(parts) < 2:
            return await msg.reply(HELP_ADGIVE)
        # target in parts[1]
        # duration in parts[2] optional
        dur_token = parts[2] if len(parts) >= 3 else None
        # resolve target
        target_id = await get_target_from_command(msg)
        target_name, _ = await get_target_name_username_from_reply(msg)
        if target_id is None:
            return await msg.reply("❌ Не смог определить пользователя. Ответь на сообщение или укажи @user/ID.")

    dur_sec = parse_duration(dur_token)
    until_ts = None if dur_sec is None else ts() + dur_sec

    permit_set(msg.chat.id, target_id, until_ts)

    await msg.reply(
        "✅ <b>Разрешение на рекламу выдано</b>\n"
        f"Пользователь: {user_link(target_id, target_name or str(target_id))}\n"
        f"⏳ До: <b>{fmt_dt(until_ts)}</b>"
    )

@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("adremove"))
async def cmd_adremove(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)
    if not is_admin(msg.from_user.id):
        return

    # reply or arg
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target_id = msg.reply_to_message.from_user.id
        target_name = msg.reply_to_message.from_user.full_name
    else:
        parts = parse_cmd_parts(msg)
        if len(parts) < 2:
            return await msg.reply(HELP_ADREMOVE)
        target_id = await get_target_from_command(msg)
        target_name = None
        if target_id is None:
            return await msg.reply("❌ Не смог определить пользователя. Укажи @user/ID или ответь на сообщение.")

    permit_remove(msg.chat.id, target_id)
    await msg.reply(
        "🗑️ <b>Разрешение на рекламу убрано</b>\n"
        f"Пользователь: {user_link(target_id, target_name or str(target_id))}"
    )


# =========================
# CALLBACK: меню
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

@dp.callback_query(F.data == "vip")
async def cb_vip(cq: CallbackQuery):
    await cq.message.edit_text("⭐ <b>VIP подписка</b>\n\nПока в разработке 🙂", reply_markup=kb_back("menu"))
    await cq.answer()


# =========================
# Разрешения (ЛС)
# =========================
@dp.callback_query(F.data == "perm_menu")
async def cb_perm_menu(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await cq.message.edit_text(
        "✅ <b>Разрешения на рекламу</b>\n\n"
        "После нажатия <b>не нужны команды</b> — просто пришли:\n"
        "• <code>@username</code>\n"
        "• или <code>ID</code>\n"
        "• или <b>перешли сообщение</b> пользователя\n\n"
        "Можно указать срок: <code>@user 15m</code>",
        reply_markup=kb_perm()
    )
    await cq.answer()

async def resolve_user_id_from_input(msg: Message, raw: str | None) -> int | None:
    if msg.forward_from:
        return msg.forward_from.id
    if raw and raw.strip().isdigit():
        return int(raw.strip())
    if raw:
        t = raw.strip()
        if t.startswith("@"):
            t = t[1:]
        try:
            ch = await bot.get_chat(t)
            if ch and ch.id:
                return int(ch.id)
        except Exception:
            return None
    return None

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

    # В ЛС выдаём на все известные чаты (как раньше)
    con = db()
    rows = con.execute("SELECT chat_id FROM known_chats").fetchall()
    con.close()
    chats = [int(r[0]) for r in rows]

    if not chats:
        await msg.answer("⚠️ Я ещё не знаю чаты. Напиши что-нибудь в группе с ботом и повтори.")
        return

    for chat_id in chats:
        permit_set(chat_id, uid, until_ts)

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

    con = db()
    rows = con.execute("SELECT chat_id FROM known_chats").fetchall()
    con.close()
    chats = [int(r[0]) for r in rows]

    for chat_id in chats:
        permit_remove(chat_id, uid)

    await state.clear()
    await msg.answer(
        "🗑️ <b>Разрешение убрано</b>\n\n"
        f"🆔 <code>{uid}</code>",
        reply_markup=kb_main(True)
    )


# =========================
# Рассылка (ЛС)
# =========================
@dp.callback_query(F.data == "bc_menu")
async def cb_bc_menu(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return

    con = db()
    rows = con.execute("SELECT chat_id, title FROM known_chats ORDER BY updated_ts DESC").fetchall()
    con.close()
    chats = [(int(r[0]), str(r[1] or "")) for r in rows]

    if not chats:
        await cq.message.edit_text("📣 Нет чатов в списке. Напиши что-нибудь в группе с ботом и вернись.")
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
        await msg.answer("⚠️ Сначала выбери чат.", reply_markup=kb_main(True))
        await state.clear()
        return

    try:
        await bot.copy_message(chat_id=chat_id, from_chat_id=msg.chat.id, message_id=msg.message_id)
        await msg.answer("✅ Отправлено.", reply_markup=kb_main(True))
    except Exception as e:
        await msg.answer(f"❌ Ошибка отправки: <code>{type(e).__name__}</code>", reply_markup=kb_main(True))
    finally:
        await state.clear()


# =========================
# Support (ЛС)
# =========================
@dp.callback_query(F.data == "support_user")
async def cb_support_user(cq: CallbackQuery):
    await cq.message.edit_text(
        "☎️ <b>Связаться с админом</b>\n\n"
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
    await cq.message.edit_text(f"✍️ Напиши ответ пользователю <code>{uid}</code>:", reply_markup=kb_back("support_admin"))
    await cq.answer()

@dp.message(AdminStates.waiting_support_reply_text)
async def st_sup_reply(msg: Message, state: FSMContext):
    if msg.chat.type != "private" or not is_admin(msg.from_user.id):
        return
    data = await state.get_data()
    uid = data.get("support_uid")
    if not uid:
        await msg.answer("⚠️ Сначала выбери пользователя.", reply_markup=kb_main(True))
        await state.clear()
        return
    try:
        await bot.send_message(uid, f"💬 <b>Ответ администратора:</b>\n\n{msg.text or ''}")
        await msg.answer("✅ Отправлено.", reply_markup=kb_main(True))
    except Exception as e:
        await msg.answer(f"❌ Не удалось отправить: <code>{type(e).__name__}</code>", reply_markup=kb_main(True))
    finally:
        await state.clear()


# =========================
# PRIVATE CATCHALL
# =========================
@dp.message(F.chat.type == "private")
async def private_catchall(msg: Message):
    if msg.text and msg.text.startswith("/"):
        if msg.text not in ("/start", "/cancel", "/chatid"):
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
# ГРУППА: ВСПОМОГАТЕЛЬНОЕ
# =========================
async def try_delete(msg: Message):
    try:
        await msg.delete()
    except Exception:
        pass

async def apply_mute(chat_id: int, user_id: int, seconds: int):
    until = now_utc() + timedelta(seconds=seconds)
    perms = ChatPermissions(can_send_messages=False)
    await bot.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)

async def apply_ban(chat_id: int, user_id: int, seconds: int | None):
    until = None
    if seconds is not None:
        until = now_utc() + timedelta(seconds=seconds)
    await bot.ban_chat_member(chat_id, user_id, until_date=until)


# =========================
# ГРУППА: /mclist
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}), Command("mclist"))
async def cmd_mclist(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)

    parts = parse_cmd_parts(msg)
    page = 1
    if len(parts) >= 2 and parts[1].isdigit():
        page = max(1, int(parts[1]))

    rows, total = mc_list(msg.chat.id, page)
    max_page = max(1, (total + MC_LIST_PAGE_SIZE - 1) // MC_LIST_PAGE_SIZE)

    text = f"📋 <b>Список наказаний</b> (стр. {page}/{max_page})\n\n"
    if not rows:
        text += "Пока записей нет."
    else:
        for (uid, uname, kind, until_ts, reason, issued_ts, active) in rows:
            until_ts = int(until_ts) if until_ts is not None else None
            st = active_tag(until_ts) if int(active) == 1 else "[Неактивно]"
            u = f"@{uname}" if uname else str(uid)
            text += (
                f"• <b>{escape(u)}</b> — <code>{escape(kind)}</code>\n"
                f"  ⏳ До: <b>{fmt_dt(until_ts)}</b> {st}\n"
                f"  🧾 Причина: <i>{escape(reason or 'причина не указана')}</i>\n"
                f"  🕒 Выдано: {fmt_dt(int(issued_ts))}\n\n"
            )

    if page < max_page:
        text += f"➡️ Следующая: <code>/mclist {page+1}</code>"
    if page > 1:
        text += f"\n⬅️ Назад: <code>/mclist {page-1}</code>"

    await msg.reply(text)


# =========================
# ГРУППА: ТОЛЬКО MC-КОМАНДЫ НАКАЗАНИЙ (без /mcunlock)
# =========================
HELP_FORMS = {
    "mcwarn":   "Форма: <code>/mcwarn @user причина</code> или ответом: <code>/mcwarn причина</code>",
    "mcmute":   "Форма: <code>/mcmute @user 15m причина</code> или ответом: <code>/mcmute 15m причина</code>",
    "mcban":    "Форма: <code>/mcban @user 1d причина</code> или ответом: <code>/mcban 1d причина</code>",
    "mckick":   "Форма: <code>/mckick @user причина</code> или ответом: <code>/mckick причина</code>",
    "mcunwarn": "Форма: <code>/mcunwarn @user</code> или ответом: <code>/mcunwarn</code>",
    "mcunmute": "Форма: <code>/mcunmute @user</code> или ответом: <code>/mcunmute</code>",
    "mcunban":  "Форма: <code>/mcunban @user</code> или ответом: <code>/mcunban</code>",
}

@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.startswith("/mc"))
async def mc_commands_router(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)

    parts = parse_cmd_parts(msg)
    if not parts:
        return

    cmd = parts[0].lstrip("/").split("@")[0].lower()

    CMDS = {"mcwarn", "mcmute", "mcban", "mckick", "mcunwarn", "mcunmute", "mcunban"}
    if cmd not in CMDS:
        return

    if not is_admin(msg.from_user.id):
        return

    if cmd in HELP_FORMS and len(parts) == 1 and not msg.reply_to_message:
        return await msg.reply(HELP_FORMS[cmd])

    target = await get_target_from_command(msg)
    if target is None:
        return await msg.reply("❌ Не смог определить пользователя. Ответь на сообщение или укажи @user/ID.")

    target_name, t_uname = await get_target_name_username_from_reply(msg)
    t_link = user_link(target, target_name or str(target))

    reason = "причина не указана"
    dur_sec = None

    if cmd == "mcwarn":
        if msg.reply_to_message:
            reason = " ".join(parts[1:]).strip() or reason
        else:
            reason = " ".join(parts[2:]).strip() or reason

        cnt = admin_warn_get(msg.chat.id, target) + 1
        admin_warn_set(msg.chat.id, target, cnt)

        mc_upsert(msg.chat.id, target, t_uname, "warn", None, reason, msg.from_user.id, 1)
        await msg.reply(f"⚠️ {t_link} получает предупреждение <b>{cnt}/3</b>\nПричина: <i>{escape(reason)}</i>")

        if cnt >= ADMIN_WARN_LIMIT:
            await apply_ban(msg.chat.id, target, ADMIN_WARN_AUTOBAN_SECONDS)
            admin_warn_set(msg.chat.id, target, 0)
            mc_upsert(
                msg.chat.id, target, t_uname, "ban",
                ts() + ADMIN_WARN_AUTOBAN_SECONDS,
                "автобан за 4/3 предупреждений",
                msg.from_user.id, 1
            )
            await msg.reply(f"⛔ {t_link} получил автобан на <b>3 дня</b> (лимит 4/3). Счётчик предупреждений сброшен.")
        return

    if cmd == "mcmute":
        if msg.reply_to_message:
            dur_sec = parse_duration(parts[1]) if len(parts) >= 2 else None
            reason = " ".join(parts[2:]).strip() or reason
        else:
            dur_sec = parse_duration(parts[2]) if len(parts) >= 3 else None
            reason = " ".join(parts[3:]).strip() or reason

        if dur_sec is None:
            dur_sec = 365 * 24 * 60 * 60
            until = None
            until_txt = "Навсегда"
        else:
            until = ts() + dur_sec
            until_txt = fmt_dt(until)

        await apply_mute(msg.chat.id, target, dur_sec)
        mc_upsert(msg.chat.id, target, t_uname, "mute", until, reason, msg.from_user.id, 1)
        await msg.reply(f"🔇 {t_link} получил мут до: <b>{until_txt}</b>\nПричина: <i>{escape(reason)}</i>")
        return

    if cmd == "mcban":
        if msg.reply_to_message:
            dur_sec = parse_duration(parts[1]) if len(parts) >= 2 else None
            reason = " ".join(parts[2:]).strip() or reason
        else:
            dur_sec = parse_duration(parts[2]) if len(parts) >= 3 else None
            reason = " ".join(parts[3:]).strip() or reason

        until = None if dur_sec is None else ts() + dur_sec
        await apply_ban(msg.chat.id, target, dur_sec)
        mc_upsert(msg.chat.id, target, t_uname, "ban", until, reason, msg.from_user.id, 1)
        await msg.reply(f"⛔ {t_link} получил бан до: <b>{fmt_dt(until)}</b> {active_tag(until)}\nПричина: <i>{escape(reason)}</i>")
        return

    if cmd == "mckick":
        if msg.reply_to_message:
            reason = " ".join(parts[1:]).strip() or reason
        else:
            reason = " ".join(parts[2:]).strip() or reason

        await bot.ban_chat_member(msg.chat.id, target)
        await bot.unban_chat_member(msg.chat.id, target)
        mc_upsert(msg.chat.id, target, t_uname, "kick", ts(), reason, msg.from_user.id, 0)
        await msg.reply(f"👢 {t_link} был кикнут.\nПричина: <i>{escape(reason)}</i>")
        return

    if cmd == "mcunwarn":
        admin_warn_set(msg.chat.id, target, 0)
        mc_upsert(msg.chat.id, target, t_uname, "warn", ts(), "снято", msg.from_user.id, 0)
        await msg.reply(f"✅ Предупреждения для {t_link} сброшены.")
        return

    if cmd == "mcunmute":
        await bot.restrict_chat_member(msg.chat.id, target, permissions=FULL_PERMS)
        mc_upsert(msg.chat.id, target, t_uname, "mute", ts(), "снято", msg.from_user.id, 0)
        await msg.reply(f"✅ Мут для {t_link} снят.")
        return

    if cmd == "mcunban":
        await bot.unban_chat_member(msg.chat.id, target)
        mc_upsert(msg.chat.id, target, t_uname, "ban", ts(), "снято", msg.from_user.id, 0)
        await msg.reply(f"✅ Бан для {t_link} снят.")
        return


# =========================
# ГРУППА: АНТИ-РЕКЛАМА
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}), (F.text | F.caption))
async def anti_ads(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)

    # команды не трогаем
    if is_command_text(msg.text) or is_command_text(msg.caption):
        return

    text = msg.text or msg.caption or ""
    if not text:
        return

    ad, raw_reason = is_ad_message(text)

    # если не реклама и нет хэштега — игнор
    if (not ad) and (not has_hashtag(text)):
        return

    chat_id = msg.chat.id
    uid = msg.from_user.id
    chat_title = msg.chat.title or ""

    u_link = user_link(uid, msg.from_user.full_name)

    permit_ok, _permit_until, last_ad_ts = permit_get(chat_id, uid)

    # без разрешения, но пишет #реклама
    if (not permit_ok) and has_hashtag(text):
        await try_delete(msg)
        await bot.send_message(chat_id, f"❌ {u_link}, у вас нет разрешения на рекламу.\nПолучить: {SUPPORT_BOT}")
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, "нет разрешения, но есть #реклама")
        return

    # есть разрешение, реклама, но тег не в конце
    if permit_ok and ad and (not hashtag_at_end(text)):
        await try_delete(msg)
        await bot.send_message(
            chat_id,
            f"🗑️ {u_link}, сообщение удалено: нет тега в конце.\n"
            f"Укажите тег <b>\"{HASHTAG}\"</b> в конце."
        )
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"разрешение есть, но тег не в конце ({raw_reason})")
        return

    # есть разрешение и реклама — лимит 24ч + остаток
    if permit_ok and ad:
        if last_ad_ts and (ts() - last_ad_ts) < ADS_COOLDOWN_SECONDS:
            left = ADS_COOLDOWN_SECONDS - (ts() - last_ad_ts)
            await try_delete(msg)
            await bot.send_message(
                chat_id,
                f"⏳ {u_link}, рекламу можно отправлять раз в <b>24 часа</b>.\n"
                f"Осталось: <b>{fmt_left(left)}</b>"
            )
            log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, "лимит 24 часа")
            return
        permit_touch_last_ad(chat_id, uid)
        return

    # нет разрешения и реклама — стадии (в сообщениях причина = "реклама")
    if (not permit_ok) and ad:
        await try_delete(msg)
        stage = ad_stage_get(chat_id, uid)

        if stage == 0:
            ad_stage_set(chat_id, uid, 1)
            await bot.send_message(
                chat_id,
                "⚠️ <b>Предупреждение</b>\n"
                f"Пользователь: {u_link}\n"
                "Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Разрешение: {SUPPORT_BOT}"
            )
        elif stage == 1:
            ad_stage_set(chat_id, uid, 2)
            try:
                await apply_mute(chat_id, uid, MUTE_2_SECONDS)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                "🔇 <b>Мут 3 часа</b>\n"
                f"Пользователь: {u_link}\n"
                "Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Разрешение: {SUPPORT_BOT}"
            )
        else:
            ad_stage_set(chat_id, uid, 0)
            try:
                await apply_mute(chat_id, uid, MUTE_3_SECONDS)
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                "🔇 <b>Мут 12 часов</b>\n"
                f"Пользователь: {u_link}\n"
                "Причина: реклама\n"
                f"Правила: {RULES_LINK}\n"
                f"Разрешение: {SUPPORT_BOT}\n\n"
                "✅ Счётчик нарушений сброшен."
            )

        # в логах оставляем детальную причину
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"реклама без разрешения ({raw_reason})")
        return


# =========================
# MAIN
# =========================
async def main():
    db().close()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
