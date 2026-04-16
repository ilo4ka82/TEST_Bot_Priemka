"""
VK-бот учёта посещаемости.
Запуск: python vk_bot.py
"""
import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import logging
from datetime import datetime, date, timedelta
import pytz

from vkbottle import Bot, Keyboard, KeyboardButtonColor, Text
from vkbottle.bot import Message

import database_operations as db
from config import VK_GROUP_TOKEN, VK_ADMIN_IDS, PREDEFINED_SECTORS
from services.attendance import is_within_office_zone
from services.export import generate_excel_report
from services.state_manager import StateManager, END

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("vkbottle").setLevel(logging.DEBUG)

MOSCOW_TZ = pytz.timezone("Europe/Moscow")

bot = Bot(token=VK_GROUP_TOKEN)
sm  = StateManager()

# --- Состояния диалогов ---
ASK_FULL_NAME            = 0
ASK_DEPARTMENT           = 1
REQUEST_MANUAL_CHECKIN   = 20
SELECT_DEPT_FOR_SHIFT    = 40
SELECT_SECTOR_EXPORT     = 50
SELECT_PERIOD_EXPORT     = 51
AWAIT_EXPORT_START       = 52
AWAIT_EXPORT_END         = 53
EDIT_AWAIT_NAME          = 60
EDIT_SELECT_USER         = 61
EDIT_SELECT_PERIOD       = 62
EDIT_SELECT_SESSION      = 63
EDIT_AWAIT_NEW_TIME      = 64
EDIT_CONFIRM             = 65
AWAIT_LINK_CODE          = 70
ADMIN_MANUAL_LIST        = 80   # список ручных заявок
ADMIN_MANUAL_DETAIL      = 81   # детали одной заявки
ADMIN_MANUAL_NEW_TIME    = 82   # ввод нового времени
ADMIN_MANUAL_CONFIRM     = 83   # подтверждение решения

SESSIONS_PER_PAGE = 5

EXPORT_PERIODS = {
    "Сегодня":               "today",
    "Вчера":                 "yesterday",
    "Эта неделя":            "this_week",
    "Прошлая неделя":        "last_week",
    "Этот месяц":            "this_month",
    "Прошлый месяц":         "last_month",
    "📅 Произвольный период": "custom",
}

EDIT_PERIODS = {
    "📅 За последнюю неделю": "week",
    "📅 За последний месяц":  "month",
    "🔢 Последние 5 сессий":  "last5",
}


# ------------------------------------------------------------------ #
# Вспомогательные функции                                             #
# ------------------------------------------------------------------ #

def is_admin(vk_id: int) -> bool:
    return vk_id in VK_ADMIN_IDS


def make_sectors_keyboard() -> Keyboard:
    kb = Keyboard(one_time=True)
    for sector in PREDEFINED_SECTORS:
        kb.add(Text(sector), color=KeyboardButtonColor.PRIMARY)
        kb.row()
    kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
    return kb


def make_main_keyboard(vk_id: int) -> Keyboard:
    user = db.get_user_by_vk_id(vk_id)
    kb = Keyboard(one_time=False)
    kb.add(Text("📍 Отметить приход"), color=KeyboardButtonColor.POSITIVE)
    kb.add(Text("🚪 Отметить уход"),   color=KeyboardButtonColor.NEGATIVE)
    kb.row()
    kb.add(Text("🕐 Ручная отметка"),  color=KeyboardButtonColor.SECONDARY)
    kb.add(Text("🔗 Привязка аккаунтов"), color=KeyboardButtonColor.SECONDARY)
    if is_admin(vk_id):
        kb.row()
        kb.add(Text("👥 Заявки"),               color=KeyboardButtonColor.PRIMARY)
        kb.add(Text("📊 Экспорт"),              color=KeyboardButtonColor.PRIMARY)
        kb.row()
        kb.add(Text("👥 Кто на смене"),         color=KeyboardButtonColor.SECONDARY)
        kb.add(Text("✏️ Редактировать сессию"), color=KeyboardButtonColor.SECONDARY)
        kb.row()
        kb.add(Text("🛠️ Ручные заявки"),       color=KeyboardButtonColor.SECONDARY)
    return kb

def make_unauth_keyboard() -> Keyboard:
    kb = Keyboard(one_time=True)
    kb.add(Text("📝 Подать заявку"),      color=KeyboardButtonColor.PRIMARY)
    kb.row()
    kb.add(Text("🔗 Привязка аккаунтов"), color=KeyboardButtonColor.SECONDARY)
    return kb


async def get_vk_name(vk_id: int) -> tuple[str, str]:
    try:
        info = await bot.api.users.get(user_ids=[vk_id], fields=["screen_name"])
        return info[0].first_name, info[0].screen_name
    except Exception:
        return "Пользователь", None


async def get_user_keyboard(vk_id: int) -> Keyboard:
    user = db.get_user_by_vk_id(vk_id)
    return make_main_keyboard(vk_id) if (user and user["is_authorized"]) else make_unauth_keyboard()


# ------------------------------------------------------------------ #
# /start                                                              #
# ------------------------------------------------------------------ #

async def handle_start(message: Message):
    vk_id = message.from_id
    first_name, screen_name = await get_vk_name(vk_id)
    user = db.get_user_by_vk_id(vk_id)
    if not user:
        user = db.add_or_update_user(vk_id=vk_id, username=screen_name, first_name=first_name)
        if is_admin(vk_id) and user:
            db._authorize_by_user_id(user["user_id"], user["user_id"])
            user = db.get_user_by_vk_id(vk_id)
    if user and user["is_authorized"]:
        status = "✅ Авторизован" + (" (Администратор)" if is_admin(vk_id) else "")
        code   = user.get("link_code", "")
        await message.answer(
            f"👋 С возвращением, {first_name}!\nVK ID: {vk_id}\nСтатус: {status}\nКод привязки Telegram: {code}",
            keyboard=make_main_keyboard(vk_id),
        )
    elif user and user["application_status"] == "pending":
        await message.answer(f"👋 Привет, {first_name}!\nВаша заявка рассматривается. Ожидайте.")
    else:
        await message.answer(
            f"👋 Привет, {first_name}!\nЭтот бот — для учёта посещаемости сотрудников.\n\n"
            "Подайте заявку на доступ или привяжите Telegram-аккаунт если уже зарегистрированы там.",
            keyboard=make_unauth_keyboard(),
        )


# ------------------------------------------------------------------ #
# Роутер                                                              #
# ------------------------------------------------------------------ #

@bot.on.message()
async def dialog_router(message: Message):
    vk_id = message.from_id
    text  = (message.text or "").strip()
    logger.info(f"Сообщение от {vk_id}: {text!r}")

    # Геолокация
    if message.attachments:
        logger.info(f"Attachments: {message.attachments}")
        for att in message.attachments:
            if hasattr(att, "geo") and att.geo:
                await handle_geo(message, att.geo)
                return

    # Команды / алиасы — всегда обрабатываем
    tl = text.lower()

    if tl in ("/start", "начать"):
        await handle_start(message); return

    if tl in ("/help", "/menu", "помощь", "меню"):
        await message.answer("Выберите действие:", keyboard=await get_user_keyboard(vk_id)); return

    if tl == "/restart":
        sm.clear("vk", vk_id)
        await message.answer("🔄 Диалог сброшен.", keyboard=await get_user_keyboard(vk_id)); return

    if tl in ("/checkin", "📍 отметить приход"):
        await handle_buttons(message, "📍 Отметить приход"); return

    if tl in ("/checkout", "🚪 отметить уход"):
        await handle_buttons(message, "🚪 Отметить уход"); return

    if tl in ("/request_manual_checkin", "🕐 ручная отметка"):
        await handle_buttons(message, "🕐 Ручная отметка"); return

    if tl in ("/link", "🔗 привязка аккаунтов", "🔗 привязать telegram", "🔗 привязка аккаунтов tg/vk"):
        await handle_buttons(message, "🔗 Привязка аккаунтов"); return

    if tl in ("/on_shift", "👥 кто на смене"):
        await handle_buttons(message, "👥 Кто на смене"); return

    if tl in ("/admin_pending_users", "👥 заявки"):
        await handle_buttons(message, "👥 Заявки"); return

    if tl in ("/admin_export_attendance", "📊 экспорт"):
        await handle_buttons(message, "📊 Экспорт"); return

    if tl in ("/admin_manual_checkins", "🛠️ ручные заявки"):
        await handle_buttons(message, "🛠️ Ручные заявки"); return

    if tl == "/edit_checkout":
        await handle_buttons(message, "✏️ Редактировать сессию"); return

    # Активный диалог
    if sm.has_active_dialog("vk", vk_id):
        state = sm.get_state("vk", vk_id)

        if text.lower() in ("отмена", "❌ отмена"):
            sm.clear("vk", vk_id)
            await message.answer("Действие отменено.", keyboard=await get_user_keyboard(vk_id))
            return

        if state == AWAIT_LINK_CODE:
            await _handle_link_code(message, text); return
        if state == ASK_FULL_NAME:
            await _handle_ask_full_name(message, text); return
        if state == ASK_DEPARTMENT:
            await _handle_ask_department(message, text); return
        if state == REQUEST_MANUAL_CHECKIN:
            await _handle_manual_checkin_time(message, text); return
        if state == SELECT_DEPT_FOR_SHIFT:
            await handle_on_shift_selection(message, text); return
        if state == SELECT_SECTOR_EXPORT:
            await handle_export_sector(message, text); return
        if state == SELECT_PERIOD_EXPORT:
            await handle_export_period(message, text); return
        if state == AWAIT_EXPORT_START:
            await handle_export_start_date(message, text); return
        if state == AWAIT_EXPORT_END:
            await handle_export_end_date(message, text); return
        if state in (EDIT_AWAIT_NAME, EDIT_SELECT_USER, EDIT_SELECT_PERIOD,
                     EDIT_SELECT_SESSION, EDIT_AWAIT_NEW_TIME, EDIT_CONFIRM):
            await handle_edit_dialog(message, text); return
        if state in (ADMIN_MANUAL_LIST, ADMIN_MANUAL_DETAIL,
                     ADMIN_MANUAL_NEW_TIME, ADMIN_MANUAL_CONFIRM):
            await handle_admin_manual_dialog(message, text); return

    await handle_buttons(message, text)


# ------------------------------------------------------------------ #
# Диалоги — вспомогательные обработчики                              #
# ------------------------------------------------------------------ #

async def _handle_link_code(message: Message, text: str):
    vk_id = message.from_id
    success, msg = db.merge_users_on_link(text, vk_id=vk_id)
    sm.clear("vk", vk_id)
    await message.answer(msg, keyboard=await get_user_keyboard(vk_id))


async def _handle_ask_full_name(message: Message, text: str):
    vk_id = message.from_id
    if len(text) < 5:
        await message.answer("Введите корректное ФИО (не менее 5 символов).")
        return
    sm.set_data("vk", vk_id, "full_name", text)
    sm.set_state("vk", vk_id, ASK_DEPARTMENT)
    await message.answer(f"Отлично, {text}!\nШаг 2 из 2: выберите сектор.", keyboard=make_sectors_keyboard())


async def _handle_ask_department(message: Message, text: str):
    vk_id = message.from_id
    key = _parse_sector(text)
    if not key:
        await message.answer("Выберите сектор из предложенных.", keyboard=make_sectors_keyboard())
        return
    full_name = sm.get_data("vk", vk_id, "full_name")
    sm.clear("vk", vk_id)
    success, msg = db.submit_application_vk(vk_id, full_name, key)
    await message.answer(msg)
    if success:
        await _notify_admins(f"🔔 Новая заявка!\n{full_name} (VK ID: {vk_id})\nСектор: {key}")


async def _handle_manual_checkin_time(message: Message, text: str):
    vk_id = message.from_id
    try:
        naive = datetime.strptime(text, "%H:%M")
        now   = datetime.now(MOSCOW_TZ)
        dt    = now.replace(hour=naive.hour, minute=naive.minute, second=0, microsecond=0)
    except ValueError:
        await message.answer("Неверный формат. Введите время как ЧЧ:ММ, например: 09:30")
        return
    user = db.get_user_by_vk_id(vk_id)
    logger.info(f"Manual checkin: user={user}, dt={dt}")
    success = db.add_manual_checkin_request(
        user_id=vk_id,
        requested_checkin_time=dt,
        vk_id=vk_id,
    )
    logger.info(f"Manual checkin result: success={success}")
    sm.clear("vk", vk_id)
    if success:
        await message.answer(f"✅ Заявка на ручную отметку в {text} отправлена.", keyboard=make_main_keyboard(vk_id))
        first_name, _ = await get_vk_name(vk_id)
        await _notify_admins(f"🕐 Запрос ручной отметки!\n{first_name} (VK ID: {vk_id})\nВремя: {text}")
    else:
        await message.answer("❌ Не удалось отправить заявку.", keyboard=make_main_keyboard(vk_id))


# ------------------------------------------------------------------ #
# Кнопки главного меню                                               #
# ------------------------------------------------------------------ #

async def handle_buttons(message: Message, text: str):
    vk_id  = message.from_id
    user   = db.get_user_by_vk_id(vk_id)
    authed = user and user["is_authorized"]

    if text in ("🔗 привязка аккаунтов", "🔗 Привязка аккаунтов", "/link"):
        tg_status = "✅ Привязан" if (user and user.get("telegram_id")) else "❌ Не привязан"
        code = user.get("link_code", "—") if user else "—"
        await message.answer(
            f"🔗 Привязка аккаунтов TG/VK\n\n"
            f"Статус Telegram: {tg_status}\n\n"
            f"Ваш код VK: {code}\n"
            f"Введите его в TG-боте командой /link\n\n"
            f"Или введите код из TG-бота сюда чтобы привязать Telegram:"
        )
        sm.set_state("vk", vk_id, AWAIT_LINK_CODE)
        return
    if text == "📝 Подать заявку":
        if authed:
            await message.answer("Вы уже авторизованы.", keyboard=make_main_keyboard(vk_id)); return
        sm.set_state("vk", vk_id, ASK_FULL_NAME)
        await message.answer("Шаг 1 из 2: введите ваше ФИО (не менее 5 символов).\nДля отмены: отмена"); return
    if text == "📍 Отметить приход":
        if not authed and not is_admin(vk_id):
            await message.answer("❌ Вы не авторизованы.", keyboard=make_unauth_keyboard()); return
        from vkbottle import Keyboard as KB, OpenLink
        kb = KB(inline=True)
        kb.add(OpenLink(
            link=f"https://tabel-opk.ru/static/checkin.html?vk_id={vk_id}",
            label="📍 Отметить приход"
        ))
        await message.answer("Нажмите кнопку для отметки прихода:", keyboard=kb)
        return

    if text == "🚪 Отметить уход":
        if not authed and not is_admin(vk_id):
            await message.answer("❌ Вы не авторизованы.", keyboard=make_unauth_keyboard()); return
        success, msg = db.record_check_out_vk(vk_id)
        await message.answer(msg, keyboard=make_main_keyboard(vk_id)); return

    if text == "🕐 Ручная отметка":
        if not authed and not is_admin(vk_id):
            await message.answer("❌ Вы не авторизованы.", keyboard=make_unauth_keyboard()); return
        sm.set_state("vk", vk_id, REQUEST_MANUAL_CHECKIN)
        await message.answer("Введите время прихода в формате ЧЧ:ММ (например: 09:30).\nДля отмены: отмена"); return

    if text == "👥 Заявки":
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        pending = db.list_pending_users()
        if not pending:
            await message.answer("Нет ожидающих заявок.", keyboard=make_main_keyboard(vk_id)); return
        lines = ["📋 Ожидающие заявки:\n"]
        for u in pending[:10]:
            lines.append(f"• {u['application_full_name']} | {u['application_department']} | VK: {u['vk_id'] or '—'} | TG: {u['telegram_id'] or '—'}")
        lines.append("\nДля авторизации: авторизовать vk <VK ID>")
        await message.answer("\n".join(lines), keyboard=make_main_keyboard(vk_id)); return

    if text.lower().startswith("авторизовать vk "):
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        try:
            target_vk = int(text.split()[-1])
            admin     = db.get_user_by_vk_id(vk_id)
            admin_uid = admin["user_id"] if admin else vk_id
            success, msg = db.authorize_user_by_vk(target_vk, admin_uid)
            await message.answer(msg, keyboard=make_main_keyboard(vk_id))
            if success:
                try:
                    await bot.api.messages.send(user_id=target_vk, message="🎉 Вы авторизованы! Теперь можете использовать бота.", random_id=0)
                except Exception as e:
                    logger.error(f"Не удалось уведомить {target_vk}: {e}")
        except (ValueError, IndexError):
            await message.answer("Формат: авторизовать vk <VK ID>")
        return

    if text == "👥 Кто на смене":
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        departments = db.get_unique_user_departments()
        if not departments:
            await message.answer("В базе нет сотрудников с секторами."); return
        kb = Keyboard(one_time=True)
        for dept in departments:
            kb.add(Text(dept), color=KeyboardButtonColor.PRIMARY); kb.row()
        kb.add(Text("Все секторы"), color=KeyboardButtonColor.SECONDARY); kb.row()
        kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
        sm.set_state("vk", vk_id, SELECT_DEPT_FOR_SHIFT)
        await message.answer("Выберите сектор:", keyboard=kb); return

    if text == "📊 Экспорт":
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        kb = Keyboard(one_time=True)
        for sector in PREDEFINED_SECTORS:
            kb.add(Text(sector), color=KeyboardButtonColor.PRIMARY); kb.row()
        kb.add(Text("Все секторы"), color=KeyboardButtonColor.SECONDARY); kb.row()
        kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
        sm.set_state("vk", vk_id, SELECT_SECTOR_EXPORT)
        await message.answer("Шаг 1: выберите сектор:", keyboard=kb); return

    if text == "✏️ Редактировать сессию":
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        sm.set_state("vk", vk_id, EDIT_AWAIT_NAME)
        await message.answer("📝 Введите ФИО или часть ФИО сотрудника.\nДля отмены: отмена"); return

    if text == "🛠️ Ручные заявки":
        if not is_admin(vk_id):
            await message.answer("❌ Нет доступа."); return
        await show_manual_requests_list(message); return


# ------------------------------------------------------------------ #
# Диалог ручных заявок (аналог admin_manual_checkins)                #
# ------------------------------------------------------------------ #

async def show_manual_requests_list(message: Message):
    vk_id   = message.from_id
    pending = db.get_pending_manual_checkin_requests()
    if not pending:
        await message.answer("Нет ожидающих заявок на ручную отметку.", keyboard=make_main_keyboard(vk_id))
        sm.clear("vk", vk_id)
        return

    lines = ["🛠️ Ожидающие ручные заявки:\n"]
    for i, req in enumerate(pending, 1):
        name = req.get("application_full_name") or req.get("username") or "Неизвестный"
        try:
            t = datetime.strptime(req["requested_checkin_time"], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
        except (ValueError, TypeError):
            t = "???"
        lines.append(f"{i}. {name} — {t}")

    lines.append("\nВведите номер заявки для просмотра деталей.")
    lines.append("Или напишите: принять все")

    kb = Keyboard(one_time=True)
    for i in range(1, min(len(pending) + 1, 11)):
        kb.add(Text(str(i)), color=KeyboardButtonColor.PRIMARY)
    kb.row()
    kb.add(Text("✅ Принять все"), color=KeyboardButtonColor.POSITIVE)
    kb.row()
    kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)

    sm.set_data("vk", vk_id, "manual_requests", pending)
    sm.set_state("vk", vk_id, ADMIN_MANUAL_LIST)
    await message.answer("\n".join(lines), keyboard=kb)


async def handle_admin_manual_dialog(message: Message, text: str):
    logger.info(f"admin_manual_dialog: state={state}, text={text!r}")
    vk_id = message.from_id
    state = sm.get_state("vk", vk_id)

    if state == ADMIN_MANUAL_LIST:
        pending = sm.get_data("vk", vk_id, "manual_requests", [])

        if text == "✅ Принять все":
            admin = db.get_user_by_vk_id(vk_id)
            admin_uid = admin["user_id"] if admin else vk_id
            approved, failed = db.approve_all_pending_manual_checkins(admin_uid)
            sm.clear("vk", vk_id)
            # Уведомляем пользователей у которых есть telegram_id
            for a in approved:
                tg_user = db.get_user_by_user_id(a["user_id"])
                if tg_user and tg_user.get("telegram_id"):
                    try:
                        naive = datetime.strptime(a["checkin_time_str"], "%Y-%m-%d %H:%M:%S")
                        t_str = naive.strftime("%d.%m.%Y %H:%M")
                        await bot.api.messages.send(
                            user_id=tg_user["telegram_id"] if tg_user.get("vk_id") else None,
                            message=f"✅ Ваша заявка одобрена. Время: {t_str}",
                            random_id=0,
                        )
                    except Exception:
                        pass
            await message.answer(
                f"✅ Массовое одобрение завершено!\nОбработано: {len(approved)} шт.\nОшибок: {failed} шт.",
                keyboard=make_main_keyboard(vk_id),
            )
            return

        try:
            idx = int(text) - 1
            req = pending[idx]
        except (ValueError, IndexError):
            await message.answer("Введите номер заявки из списка.")
            return

        sm.set_data("vk", vk_id, "current_manual_req", req)
        sm.set_state("vk", vk_id, ADMIN_MANUAL_DETAIL)
        await show_manual_request_detail(message, req)
        return

    if state == ADMIN_MANUAL_DETAIL:
        req = sm.get_data("vk", vk_id, "current_manual_req")

        if text == "✅ Одобрить":
            sm.set_data("vk", vk_id, "manual_action", "approve_as_is")
            sm.set_state("vk", vk_id, ADMIN_MANUAL_CONFIRM)
            name = req.get("application_full_name") or req.get("username") or "?"
            kb = Keyboard(one_time=True)
            kb.add(Text("✅ Да, одобрить"), color=KeyboardButtonColor.POSITIVE)
            kb.add(Text("❌ Нет"), color=KeyboardButtonColor.NEGATIVE)
            await message.answer(f"Одобрить заявку для {name} как есть?", keyboard=kb)
            return

        if text == "🕒 Изменить время":
            sm.set_data("vk", vk_id, "manual_action", "approve_new_time")
            sm.set_state("vk", vk_id, ADMIN_MANUAL_NEW_TIME)
            await message.answer("Введите новое время в формате ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 07.04.2026 09:00\n\nДля отмены: отмена")
            return

        if text == "❌ Отклонить":
            sm.set_data("vk", vk_id, "manual_action", "reject")
            sm.set_state("vk", vk_id, ADMIN_MANUAL_CONFIRM)
            name = req.get("application_full_name") or req.get("username") or "?"
            kb = Keyboard(one_time=True)
            kb.add(Text("✅ Да, отклонить"), color=KeyboardButtonColor.NEGATIVE)
            kb.add(Text("❌ Нет"), color=KeyboardButtonColor.SECONDARY)
            await message.answer(f"Отклонить заявку для {name}?", keyboard=kb)
            return

        if text == "« Назад":
            await show_manual_requests_list(message)
            return

        return

    if state == ADMIN_MANUAL_NEW_TIME:
        try:
            new_dt = datetime.strptime(text, "%d.%m.%Y %H:%M")
            aware  = MOSCOW_TZ.localize(new_dt)
        except ValueError:
            await message.answer("❌ Неверный формат. Введите как ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 07.04.2026 09:00")
            return
        sm.set_data("vk", vk_id, "new_manual_time", aware)
        sm.set_state("vk", vk_id, ADMIN_MANUAL_CONFIRM)
        req  = sm.get_data("vk", vk_id, "current_manual_req")
        name = req.get("application_full_name") or req.get("username") or "?"
        kb = Keyboard(one_time=True)
        kb.add(Text("✅ Да, одобрить"), color=KeyboardButtonColor.POSITIVE)
        kb.add(Text("❌ Нет"), color=KeyboardButtonColor.NEGATIVE)
        await message.answer(
            f"Одобрить заявку для {name} с новым временем {aware.strftime('%d.%m.%Y %H:%M')}?",
            keyboard=kb,
        )
        return

    if state == ADMIN_MANUAL_CONFIRM:
        action = sm.get_data("vk", vk_id, "manual_action")
        req    = sm.get_data("vk", vk_id, "current_manual_req")
        admin  = db.get_user_by_vk_id(vk_id)
        admin_uid = admin["user_id"] if admin else vk_id

        if text in ("✅ Да, одобрить", "✅ Да, отклонить") and "Да" in text:
            if action == "reject":
                db.reject_manual_checkin_request(req["request_id"], admin_uid)
                sm.clear("vk", vk_id)
                name = req.get("application_full_name") or "?"
                await message.answer(f"❌ Заявка от {name} отклонена.", keyboard=make_main_keyboard(vk_id))
                # Уведомить пользователя если есть VK ID
                if req.get("vk_id"):
                    try:
                        await bot.api.messages.send(user_id=req["vk_id"], message="❌ Ваша заявка на ручную отметку была отклонена.", random_id=0)
                    except Exception:
                        pass
            else:
                if action == "approve_new_time":
                    checkin_time = sm.get_data("vk", vk_id, "new_manual_time")
                else:
                    naive = datetime.strptime(req["requested_checkin_time"], "%Y-%m-%d %H:%M:%S")
                    checkin_time = MOSCOW_TZ.localize(naive)

                db.approve_manual_checkin_request(
                    req["request_id"], admin_uid, checkin_time,
                    req["user_id"], req.get("application_department")
                )
                t_str = checkin_time.strftime("%d.%m.%Y %H:%M")
                sm.clear("vk", vk_id)
                name = req.get("application_full_name") or "?"
                await message.answer(f"✅ Заявка от {name} одобрена. Время: {t_str}", keyboard=make_main_keyboard(vk_id))
                if req.get("vk_id"):
                    try:
                        await bot.api.messages.send(user_id=req["vk_id"], message=f"✅ Ваша заявка одобрена. Время: {t_str}", random_id=0)
                    except Exception:
                        pass
        else:
            # Нет — возвращаемся к деталям
            sm.set_state("vk", vk_id, ADMIN_MANUAL_DETAIL)
            await show_manual_request_detail(message, req)
        return


async def show_manual_request_detail(message: Message, req: dict):
    name = req.get("application_full_name") or req.get("username") or "Неизвестный"
    dept = req.get("application_department") or "Не указан"
    try:
        t = datetime.strptime(req["requested_checkin_time"], "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y в %H:%M")
    except (ValueError, TypeError):
        t = "???"

    kb = Keyboard(one_time=True)
    kb.add(Text("✅ Одобрить"),    color=KeyboardButtonColor.POSITIVE)
    kb.add(Text("🕒 Изменить время"), color=KeyboardButtonColor.SECONDARY)
    kb.row()
    kb.add(Text("❌ Отклонить"),   color=KeyboardButtonColor.NEGATIVE)
    kb.add(Text("« Назад"),        color=KeyboardButtonColor.SECONDARY)

    await message.answer(
        f"📋 Заявка #{req['request_id']}\n\n"
        f"Сотрудник: {name}\nСектор: {dept}\nВремя: {t}\n\nВыберите действие:",
        keyboard=kb,
    )


# ------------------------------------------------------------------ #
# Геолокация                                                          #
# ------------------------------------------------------------------ #

async def handle_geo(message: Message, geo):
    vk_id = message.from_id
    user  = db.get_user_by_vk_id(vk_id)
    if not user or (not user["is_authorized"] and not is_admin(vk_id)):
        await message.answer("❌ Вы не авторизованы."); return
    try:
        lat = geo.coordinates.latitude
        lon = geo.coordinates.longitude
    except AttributeError:
        await message.answer("❌ Не удалось получить координаты."); return
    if not is_within_office_zone(lat, lon):
        await message.answer("❌ Вы слишком далеко от офиса.", keyboard=make_main_keyboard(vk_id)); return
    success, msg = db.record_check_in_vk(vk_id, lat, lon)
    await message.answer(msg, keyboard=make_main_keyboard(vk_id))


# ------------------------------------------------------------------ #
# Кто на смене                                                        #
# ------------------------------------------------------------------ #

async def handle_on_shift_selection(message: Message, text: str):
    vk_id = message.from_id
    dept  = "ALL" if text == "Все секторы" else text
    sm.clear("vk", vk_id)
    active = db.get_active_users_by_department(dept)
    header = "Все" if dept == "ALL" else dept
    lines  = [f"👥 На смене ({header}):\n"]
    if not active:
        lines.append("На смене никого нет.")
        await message.answer("\n".join(lines), keyboard=make_main_keyboard(vk_id)); return
    grouped = {}
    for u in active:
        name = u["application_full_name"] or u["username"] or "Без имени"
        d    = u["application_department"] or "Без сектора"
        try:
            t = datetime.strptime(u["check_in_time"], "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
        except (ValueError, TypeError):
            t = "??:??"
        grouped.setdefault(d, []).append((name, t))
    max_len = max(len(n) for lst in grouped.values() for n, _ in lst)
    for d, employees in sorted(grouped.items()):
        lines.append(f"\n{d}:")
        for name, t in employees:
            lines.append(f"  {name.ljust(max_len)}  |  {t}")
    await message.answer("\n".join(lines), keyboard=make_main_keyboard(vk_id))


# ------------------------------------------------------------------ #
# Экспорт                                                             #
# ------------------------------------------------------------------ #

async def handle_export_sector(message: Message, text: str):
    vk_id = message.from_id
    if text == "Все секторы":
        key, display = "ALL", "Все секторы"
    else:
        key = _parse_sector(text)
        display = text
        if not key:
            await message.answer("Выберите сектор из предложенных."); return
    sm.set_data("vk", vk_id, "export_sector_key",     key)
    sm.set_data("vk", vk_id, "export_sector_display",  display)
    sm.set_state("vk", vk_id, SELECT_PERIOD_EXPORT)
    kb = Keyboard(one_time=True)
    for name in EXPORT_PERIODS:
        kb.add(Text(name), color=KeyboardButtonColor.PRIMARY); kb.row()
    kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
    await message.answer("Шаг 2: выберите период:", keyboard=kb)


async def handle_export_period(message: Message, text: str):
    vk_id = message.from_id
    if text not in EXPORT_PERIODS:
        await message.answer("Выберите период из предложенных."); return
    period_type = EXPORT_PERIODS[text]
    sm.set_data("vk", vk_id, "export_period_display", text)
    if period_type == "custom":
        sm.set_state("vk", vk_id, AWAIT_EXPORT_START)
        await message.answer("Введите начальную дату в формате ДД.ММ.ГГГГ:\nДля отмены: отмена"); return
    today = date.today()
    if period_type == "today":        start = end = today
    elif period_type == "yesterday":  start = end = today - timedelta(days=1)
    elif period_type == "this_week":  start = today - timedelta(days=today.weekday()); end = today
    elif period_type == "last_week":  start = today - timedelta(days=today.weekday() + 7); end = start + timedelta(days=6)
    elif period_type == "this_month": start = today.replace(day=1); end = today
    elif period_type == "last_month": end = today.replace(day=1) - timedelta(days=1); start = end.replace(day=1)
    sm.set_data("vk", vk_id, "export_start_date", start)
    sm.set_data("vk", vk_id, "export_end_date",   end)
    await run_export(message)


async def handle_export_start_date(message: Message, text: str):
    vk_id = message.from_id
    try:
        start = datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer("Неверный формат. Введите как ДД.ММ.ГГГГ, например: 01.01.2025"); return
    sm.set_data("vk", vk_id, "export_start_date", start)
    sm.set_state("vk", vk_id, AWAIT_EXPORT_END)
    await message.answer("Теперь введите конечную дату в формате ДД.ММ.ГГГГ:")


async def handle_export_end_date(message: Message, text: str):
    vk_id = message.from_id
    try:
        end = datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        await message.answer("Неверный формат. Введите как ДД.ММ.ГГГГ, например: 31.01.2025"); return
    start = sm.get_data("vk", vk_id, "export_start_date")
    if end < start:
        await message.answer("Конечная дата не может быть раньше начальной. Введите снова:"); return
    sm.set_data("vk", vk_id, "export_end_date", end)
    await run_export(message)


async def run_export(message: Message):
    vk_id   = message.from_id
    key     = sm.get_data("vk", vk_id, "export_sector_key")
    display = sm.get_data("vk", vk_id, "export_sector_display")
    start   = sm.get_data("vk", vk_id, "export_start_date")
    end     = sm.get_data("vk", vk_id, "export_end_date")
    period  = sm.get_data("vk", vk_id, "export_period_display")
    sm.clear("vk", vk_id)
    await message.answer(f"⏳ Формирую отчёт ({display}, {period})...")
    start_dt    = datetime.combine(start, datetime.min.time())
    end_dt      = datetime.combine(end,   datetime.max.time())
    data        = db.get_attendance_data_for_period(start_dt, end_dt, None if key == "ALL" else key)
    excel_bytes = await generate_excel_report(data, {"sector_display_name": display}, key)
    if not excel_bytes:
        await message.answer("❌ Не удалось сформировать отчёт.", keyboard=make_main_keyboard(vk_id)); return
    import aiohttp
    upload_server = await bot.api.docs.get_messages_upload_server(type="doc", peer_id=vk_id)
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field("file", excel_bytes, filename=f"report_{start}_{end}.xlsx")
        async with session.post(upload_server.upload_url, data=form) as resp:
            upload_result = await resp.json()
    saved = await bot.api.docs.save(file=upload_result["file"], title=f"Отчёт {display} {period}")
    doc   = saved.doc
    await bot.api.messages.send(user_id=vk_id, attachment=f"doc{doc.owner_id}_{doc.id}", message=f"📊 Отчёт готов: {display}, {period}", random_id=0)


# ------------------------------------------------------------------ #
# Редактирование сессий                                               #
# ------------------------------------------------------------------ #

def _build_session_text(user_name: str, sessions: list, offset: int) -> str:
    total = len(sessions)
    page  = sessions[offset: offset + SESSIONS_PER_PAGE]
    lines = [f"Сессии для {user_name} ({offset+1}–{offset+len(page)} из {total}):\n"]
    for i, s in enumerate(page, start=offset + 1):
        try:
            ci = datetime.strptime(s["check_in_time"],  "%Y-%m-%d %H:%M:%S")
            co = datetime.strptime(s["check_out_time"], "%Y-%m-%d %H:%M:%S")
            if ci.date() == co.date():
                line = f"{i}. {ci.strftime('%d.%m')} | {ci.strftime('%H:%M')} – {co.strftime('%H:%M')}"
            else:
                line = f"{i}. {ci.strftime('%d.%m')} – {co.strftime('%d.%m')} | {ci.strftime('%H:%M')} – {co.strftime('%H:%M')}"
        except (ValueError, TypeError):
            line = f"{i}. Ошибка данных (ID: {s['session_id']})"
        lines.append(line)
    return "\n".join(lines)


def _build_session_keyboard(sessions: list, offset: int) -> Keyboard:
    kb   = Keyboard(one_time=True)
    page = sessions[offset: offset + SESSIONS_PER_PAGE]
    for i in range(offset + 1, offset + len(page) + 1):
        kb.add(Text(str(i)), color=KeyboardButtonColor.PRIMARY)
    kb.row()
    if offset > 0:
        kb.add(Text("⬅️ Назад"), color=KeyboardButtonColor.SECONDARY)
    if offset + SESSIONS_PER_PAGE < len(sessions):
        kb.add(Text("➡️ Вперёд"), color=KeyboardButtonColor.SECONDARY)
    kb.row()
    kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
    return kb


async def _get_sessions_for_user(user_id: int, period: str) -> list:
    import sqlite3
    from config import DATABASE_PATH
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT session_id, check_in_time, check_out_time
        FROM work_sessions WHERE user_id = ? AND check_out_time IS NOT NULL
        ORDER BY check_in_time DESC
    """, (user_id,))
    all_sessions = [dict(r) for r in cursor.fetchall()]
    conn.close()
    if period == "last5":
        return all_sessions[:5]
    days   = 7 if period == "week" else 30
    cutoff = datetime.now(MOSCOW_TZ) - timedelta(days=days)
    result = []
    for s in all_sessions:
        try:
            dt = MOSCOW_TZ.localize(datetime.strptime(s["check_in_time"], "%Y-%m-%d %H:%M:%S"))
            if dt >= cutoff:
                result.append(s)
        except (ValueError, TypeError):
            continue
    return result


async def handle_edit_dialog(message: Message, text: str):
    vk_id = message.from_id
    state = sm.get_state("vk", vk_id)

    if state == EDIT_AWAIT_NAME:
        found = db.find_users_by_name(text)
        if not found:
            await message.answer(f"❌ Сотрудник '{text}' не найден. Попробуйте ещё раз."); return
        if len(found) == 1:
            u = found[0]
            sm.set_data("vk", vk_id, "edit_target_user_id",   u["user_id"])
            sm.set_data("vk", vk_id, "edit_target_user_name", u["application_full_name"])
            sm.set_state("vk", vk_id, EDIT_SELECT_PERIOD)
            kb = Keyboard(one_time=True)
            for p in EDIT_PERIODS:
                kb.add(Text(p), color=KeyboardButtonColor.PRIMARY); kb.row()
            kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
            await message.answer(f"✅ Выбран: {u['application_full_name']}\n\nЗа какой период?", keyboard=kb); return
        sm.set_data("vk", vk_id, "edit_found_users", found)
        sm.set_state("vk", vk_id, EDIT_SELECT_USER)
        kb = Keyboard(one_time=True)
        lines = ["Найдено несколько. Введите номер:\n"]
        for i, u in enumerate(found, 1):
            lines.append(f"{i}. {u['application_full_name']}")
            kb.add(Text(str(i)), color=KeyboardButtonColor.PRIMARY)
        kb.row(); kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
        await message.answer("\n".join(lines), keyboard=kb); return

    if state == EDIT_SELECT_USER:
        found = sm.get_data("vk", vk_id, "edit_found_users", [])
        try:
            u = found[int(text) - 1]
        except (ValueError, IndexError):
            await message.answer("Введите номер из списка."); return
        sm.set_data("vk", vk_id, "edit_target_user_id",   u["user_id"])
        sm.set_data("vk", vk_id, "edit_target_user_name", u["application_full_name"])
        sm.set_state("vk", vk_id, EDIT_SELECT_PERIOD)
        kb = Keyboard(one_time=True)
        for p in EDIT_PERIODS:
            kb.add(Text(p), color=KeyboardButtonColor.PRIMARY); kb.row()
        kb.add(Text("❌ Отмена"), color=KeyboardButtonColor.NEGATIVE)
        await message.answer(f"✅ Выбран: {u['application_full_name']}\n\nЗа какой период?", keyboard=kb); return

    if state == EDIT_SELECT_PERIOD:
        if text not in EDIT_PERIODS:
            await message.answer("Выберите период из предложенных."); return
        target_uid  = sm.get_data("vk", vk_id, "edit_target_user_id")
        target_name = sm.get_data("vk", vk_id, "edit_target_user_name")
        sessions = await _get_sessions_for_user(target_uid, EDIT_PERIODS[text])
        if not sessions:
            await message.answer(f"У {target_name} нет завершённых сессий за этот период.", keyboard=make_main_keyboard(vk_id))
            sm.clear("vk", vk_id); return
        sm.set_data("vk", vk_id, "edit_sessions_list",   sessions)
        sm.set_data("vk", vk_id, "edit_sessions_offset", 0)
        sm.set_state("vk", vk_id, EDIT_SELECT_SESSION)
        await message.answer(_build_session_text(target_name, sessions, 0), keyboard=_build_session_keyboard(sessions, 0)); return

    if state == EDIT_SELECT_SESSION:
        sessions    = sm.get_data("vk", vk_id, "edit_sessions_list", [])
        offset      = sm.get_data("vk", vk_id, "edit_sessions_offset", 0)
        target_name = sm.get_data("vk", vk_id, "edit_target_user_name")
        if text == "⬅️ Назад" and offset > 0:
            new_offset = max(0, offset - SESSIONS_PER_PAGE)
            sm.set_data("vk", vk_id, "edit_sessions_offset", new_offset)
            await message.answer(_build_session_text(target_name, sessions, new_offset), keyboard=_build_session_keyboard(sessions, new_offset)); return
        if text == "➡️ Вперёд" and offset + SESSIONS_PER_PAGE < len(sessions):
            new_offset = offset + SESSIONS_PER_PAGE
            sm.set_data("vk", vk_id, "edit_sessions_offset", new_offset)
            await message.answer(_build_session_text(target_name, sessions, new_offset), keyboard=_build_session_keyboard(sessions, new_offset)); return
        try:
            session = sessions[int(text) - 1]
        except (ValueError, IndexError):
            await message.answer("Введите номер сессии из списка."); return
        sm.set_data("vk", vk_id, "edit_target_session_id", session["session_id"])
        sm.set_state("vk", vk_id, EDIT_AWAIT_NEW_TIME)
        try:
            co  = datetime.strptime(session["check_out_time"], "%Y-%m-%d %H:%M:%S")
            cur = co.strftime("%d.%m.%Y %H:%M"); d = co.strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            cur = d = "?"
        await message.answer(f"Сессия от {d}.\nТекущее время ухода: {cur}\n\nВведите новое время в формате ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: {d} 18:30\n\nДля отмены: отмена"); return

    if state == EDIT_AWAIT_NEW_TIME:
        try:
            new_dt = datetime.strptime(text, "%d.%m.%Y %H:%M")
        except ValueError:
            await message.answer("❌ Неверный формат. Введите как ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 27.07.2025 18:30"); return
        sm.set_data("vk", vk_id, "edit_new_checkout_dt", new_dt)
        sm.set_state("vk", vk_id, EDIT_CONFIRM)
        target_name = sm.get_data("vk", vk_id, "edit_target_user_name")
        kb = Keyboard(one_time=True)
        kb.add(Text("✅ Да, изменить"), color=KeyboardButtonColor.POSITIVE)
        kb.add(Text("❌ Нет, назад"),   color=KeyboardButtonColor.NEGATIVE)
        await message.answer(f"Изменить время ухода для {target_name} на {new_dt.strftime('%d.%m.%Y %H:%M')}?", keyboard=kb); return

    if state == EDIT_CONFIRM:
        if text == "✅ Да, изменить":
            session_id = sm.get_data("vk", vk_id, "edit_target_session_id")
            new_dt     = sm.get_data("vk", vk_id, "edit_new_checkout_dt")
            success    = db.update_session_checkout_time(session_id, new_dt.strftime("%Y-%m-%d %H:%M:%S"))
            sm.clear("vk", vk_id)
            msg = "✅ Время ухода успешно изменено!" if success else "❌ Ошибка при обновлении."
            await message.answer(msg, keyboard=make_main_keyboard(vk_id))
        else:
            sm.set_state("vk", vk_id, EDIT_AWAIT_NEW_TIME)
            await message.answer("Введите новое время в формате ДД.ММ.ГГГГ ЧЧ:ММ\nДля отмены: отмена")
        return


# ------------------------------------------------------------------ #
# Утилиты                                                             #
# ------------------------------------------------------------------ #

def _parse_sector(text: str) -> str | None:
    for predefined in PREDEFINED_SECTORS:
        parts = predefined.split()
        key   = parts[-1].upper() if len(parts) > 1 else predefined.upper()
        if text == predefined or text.upper() == key:
            return key
    return None


async def _notify_admins(text: str):
    for admin_id in VK_ADMIN_IDS:
        try:
            await bot.api.messages.send(user_id=admin_id, message=text, random_id=0)
        except Exception as e:
            logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")


if __name__ == "__main__":
    db.init_db()
    from config import DATABASE_PATH
    logger.info(f"БД путь: {DATABASE_PATH}")
    logger.info("VK-бот запускается...")
    bot.run_forever()