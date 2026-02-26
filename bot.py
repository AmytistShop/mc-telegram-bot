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
    raise RuntimeError("TOKEN is not set. Add environment variable TOKEN in Render.")

ADMIN_IDS = {8085895186}

DB_PATH = "mc_bot.db"
HASHTAG = "#реклама"

# анти-реклама: стадии наказаний бота
MUTE_2_SECONDS = 3 * 60 * 60       # 3 часа
MUTE_3_SECONDS = 12 * 60 * 60      # 12 часов

# лимит рекламы по разрешению
ADS_COOLDOWN_SECONDS = 24 * 60 * 60

# админские предупреждения: 4/3 => бан 3 дня
ADMIN_WARN_LIMIT = 4
ADMIN_WARN_AUTOBAN_SECONDS = 3 * 24 * 60 * 60

RULES_LINK = "https://leoned777.github.io/chats/"
SUPPORT_BOT_FOR_PERMIT = "@minecrfat_bot"  # как ты попросил

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


# =========================
# АНТИ-РЕКЛАМА (правила)
# =========================
KW = ["продам", "куплю", "сдам", "прайс", "подпишитесь", "подписывайтесь"]
TELEGRAM_LINK = re.compile(r"(https?://)?t\.me/[\w_]{3,}", re.I)
PHONE = re.compile(r"(\+?\d[\d\-\s\(\)]{8,}\d)")

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
        kind TEXT NOT NULL,             -- warn/mute/ban/kick
        until_ts INTEGER,               -- null=навсегда
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
# КЛАВИАТУРЫ (ЛС)
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
        [InlineKeyboardButton(text="☎️ Связь с админом", callback_data="support_user")],
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
# УДАЛЕНИЕ СООБЩЕНИЙ (честно)
# =========================
async def try_delete(msg: Message) -> bool:
    try:
        await msg.delete()
        return True
    except Exception:
        return False

async def ensure_delete_warning(chat_id: int):
    # коротко и по делу — чтобы ты понимал почему "не удаляет"
    await bot.send_message(
        chat_id,
        "⚠️ Я не смог удалить сообщение.\n"
        "Дай мне права: <b>Delete messages</b> (сделай админом) и включи удаление сообщений."
    )


# =========================
# ГРУППА: наказания (mute/ban)
# =========================
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
# /start /cancel /chatid /userid
# =========================
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        "🏠 <b>Главное меню</b>\n\n"
        f"📌 Тег рекламы в конце: <code>{HASHTAG}</code>\n"
        f"📖 Правила: {RULES_LINK}",
        reply_markup=kb_main(is_admin(msg.from_user.id))
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

@dp.message(Command("userid"))
async def cmd_userid(msg: Message):
    # reply -> id цели
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        await msg.reply(f"🆔 ID пользователя: <code>{u.id}</code>")
        return
    # forward -> id (если не скрыто)
    if msg.forward_from:
        await msg.reply(f"🆔 ID пользователя: <code>{msg.forward_from.id}</code>")
        return
    # иначе свой
    await msg.reply(f"🆔 Твой ID: <code>{msg.from_user.id}</code>")


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
# Разрешения (ЛС без команд)
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

    chats = get_known_chats()
    if not chats:
        await msg.answer("⚠️ Я ещё не знаю чаты. Напиши что-нибудь в группе с ботом и повтори.")
        return

    for chat_id, _ in chats:
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

    chats = get_known_chats()
    for chat_id, _ in chats:
        permit_remove(chat_id, uid)

    await state.clear()
    await msg.answer(
        "🗑️ <b>Разрешение убрано</b>\n\n"
        f"🆔 <code>{uid}</code>",
        reply_markup=kb_main(True)
    )


# =========================
# Рассылка (ЛС без /bc)
# =========================
@dp.callback_query(F.data == "bc_menu")
async def cb_bc_menu(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        await cq.answer("Нет доступа", show_alert=True)
        return
    chats = get_known_chats()
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
    finally:
        await state.clear()


# =========================
# PRIVATE CATCHALL
# =========================
@dp.message(F.chat.type == "private")
async def private_catchall(msg: Message):
    # неизвестные команды в ЛС
    if msg.text and msg.text.startswith("/"):
        if msg.text not in ("/start", "/cancel", "/chatid", "/userid"):
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
# ГРУППА: АНТИ-РЕКЛАМА (НЕ ТРОГАЕТ КОМАНДЫ)
# =========================
@dp.message(F.chat.type.in_({"group", "supergroup"}) & (F.text | F.caption))
async def anti_ads(msg: Message):
    remember_chat(msg.chat.id, msg.chat.title)

    # команды не трогаем
    if is_command_text(msg.text) or is_command_text(msg.caption):
        return

    text = msg.text or msg.caption or ""
    if not text:
        return

    ad, reason = is_ad_message(text)

    # если не реклама и нет хэштега — игнор
    if (not ad) and (not has_hashtag(text)):
        return

    chat_id = msg.chat.id
    uid = msg.from_user.id
    chat_title = msg.chat.title or ""

    permit_ok, permit_until, last_ad_ts = permit_get(chat_id, uid)

    # (1) без разрешения, но пишет #реклама
    if (not permit_ok) and has_hashtag(text):
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)
        await bot.send_message(chat_id, f"❌ У вас нет разрешения на рекламу.\nПолучить: {SUPPORT_BOT_FOR_PERMIT}")
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, "нет разрешения, но есть #реклама")
        return

    # (2) есть разрешение, но реклама без тега в конце
    if permit_ok and ad and (not hashtag_at_end(text)):
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)
        await bot.send_message(
            chat_id,
            "🗑️ Ваше сообщение удалено, по причине отсутствия тега на рекламу.\n"
            f"Пожалуйста укажите тег <b>\"{HASHTAG}\"</b> <b>в конце</b>."
        )
        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"разрешение есть, но тег не в конце ({reason})")
        return

    # (3) есть разрешение и реклама — лимит 24ч
    if permit_ok and ad:
        if last_ad_ts and (ts() - last_ad_ts) < ADS_COOLDOWN_SECONDS:
            deleted = await try_delete(msg)
            if not deleted:
                await ensure_delete_warning(chat_id)
            await bot.send_message(chat_id, "⏳ Рекламу можно отправлять раз в <b>24 часа</b>.")
            log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, "лимит 24 часа")
            return
        permit_touch_last_ad(chat_id, uid)
        return

    # (4) нет разрешения и реклама — стадии
    if (not permit_ok) and ad:
        deleted = await try_delete(msg)
        if not deleted:
            await ensure_delete_warning(chat_id)

        stage = ad_stage_get(chat_id, uid)

        if stage == 0:
            ad_stage_set(chat_id, uid, 1)
            await bot.send_message(
                chat_id,
                f"⚠️ Реклама без разрешения запрещена.\n"
                f"Причина: {reason}\n"
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
                f"🔇 Мут на <b>3 часа</b>.\n"
                f"Причина: {reason}\n"
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
                f"🔇 Мут на <b>12 часов</b>.\n"
                f"Причина: {reason}\n"
                f"Правила: {RULES_LINK}\n"
                f"Получить разрешение можно в боте: {SUPPORT_BOT_FOR_PERMIT}\n"
                f'В разделе "Связь с админом".\n\n'
                f"✅ Счётчик нарушений сброшен."
            )

        log_deleted_ad(chat_id, chat_title, uid, msg.from_user.username, text, f"реклама без разрешения ({reason})")
        return


# =========================
# Команды для подсказок "/"
# =========================
async def setup_commands():
    cmds = [
        BotCommand(command="start", description="Меню бота"),
        BotCommand(command="cancel", description="Отмена/выход в меню"),
        BotCommand(command="chatid", description="Показать chat_id (в группе)"),
        BotCommand(command="userid", description="Узнать ID (reply/forward/свой)"),

        BotCommand(command="mclist", description="Список наказаний (страницы: /mclist 2)"),
        BotCommand(command="mcwarn", description="Предупреждение (reply или @user)"),
        BotCommand(command="mcmute", description="Мут (reply или @user)"),
        BotCommand(command="mcban", description="Бан (reply или @user)"),
        BotCommand(command="mckick", description="Кик (reply или @user)"),
        BotCommand(command="mcunwarn", description="Снять предупреждения"),
        BotCommand(command="mcunmute", description="Снять мут"),
        BotCommand(command="mcunban", description="Снять бан"),
        BotCommand(command="mcunlock", description="Снять бан/мут"),
    ]

    # одинаковые команды по умолчанию (Telegram покажет их при "/")
    await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())
    await bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())


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
