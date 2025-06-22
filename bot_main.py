import logging
import os
import sys
import math
import io
import pandas as pd
from datetime import datetime, timedelta, date
from datetime import timezone 
from zoneinfo import ZoneInfo
from typing import Union, Dict 
import telegram
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, BotCommand, BotCommandScopeChat, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram import Update, User, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ConversationHandler
from openpyxl.styles import Font
import config
from config import TELEGRAM_BOT_TOKEN, ADMIN_TELEGRAM_IDS, THE_OFFICE_ZONE, ITEMS_PER_PAGE, SECTOR_WEEKLY_NORMS 
from telegram_bot_calendar import DetailedTelegramCalendar, LSTEP 
from telegram.ext.filters import BaseFilter
import database_operations as db


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def is_admin(user_id: int) -> bool:
    """
    Проверяет, является ли пользователь администратором,
    сравнивая его ID со списком из файла конфигурации.
    """
    return user_id in config.ADMIN_TELEGRAM_IDS

utc = timezone.utc
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# Состояния для ConversationHandler экспорта
EXPORT_CONV_START_STATE = 10 
(SELECT_SECTOR, 
 SELECT_PERIOD, 
 GET_EXPORT_START_DATE, 
 GET_EXPORT_END_DATE,    
 CONFIRM_EXPORT) = range(EXPORT_CONV_START_STATE, EXPORT_CONV_START_STATE + 5)

# Состояния для диалога подачи заявки
# Убедимся, что нумерация не пересекается. Если EXPORT_CONV_START_STATE + 5 = 15, то начнем с 15.
# Или, если ASK_FULL_NAME, ASK_DEPARTMENT уже используются и имеют значения 0, 1,
# то нужно выбрать диапазон, который не конфликтует.
# Давайте предположим, что для заявок на доступ используются 0 и 1.
# Тогда для экспорта используются 10-14.
# Новое состояние для ручной отметки может начаться, например, с 20.

APP_CONV_START_STATE = 0 # Предположим, что для заявок на доступ так
ASK_FULL_NAME, ASK_DEPARTMENT = range(APP_CONV_START_STATE, APP_CONV_START_STATE + 2) # Будут 0, 1

# Новое состояние для запроса времени ручной отметки прихода
MANUAL_CHECKIN_CONV_START_STATE = 20 # Выбираем новый диапазон
REQUEST_MANUAL_CHECKIN_TIME = MANUAL_CHECKIN_CONV_START_STATE # Будет 20

# Состояния для административного диалога обработки ручных заявок
ADMIN_MANUAL_CHECKINS_STATE_START = 30 # Выбираем новый диапазон, не пересекающийся с другими
(
    ADMIN_LIST_MANUAL_REQUESTS,      # Отображение списка заявок
    ADMIN_PROCESS_SINGLE_REQUEST,    # Выбор действия по конкретной заявке
    ADMIN_ENTER_NEW_TIME,            # Ввод нового времени админом
    ADMIN_CONFIRM_REQUEST_DECISION   # Финальное подтверждение решения
) = range(ADMIN_MANUAL_CHECKINS_STATE_START, ADMIN_MANUAL_CHECKINS_STATE_START + 4)
# Это будут состояния 30, 31, 32, 33

PREDEFINED_SECTORS = ["Сектор СС", "Сектор ВИ", "Сектор ОП"]


class AdminFilter(BaseFilter):
    def filter(self, message: Union[Dict, 'telegram.Message']) -> bool:
        # Проверяем, что ID пользователя есть в списке админов
        return message.from_user.id in ADMIN_TELEGRAM_IDS

def escape_markdown_v2(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return "".join(f'\\{char}' if char in escape_chars else char for char in text)

def admin_required(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in ADMIN_TELEGRAM_IDS:
            error_message = escape_markdown_v2("❌ Эта команда доступна только администратору.")
            await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
            logger.warning(f"Несанкционированный доступ к админ-команде от {user_id} ({update.effective_user.username})")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    db.add_or_update_user(user.id, user.username, user.first_name, user.last_name)
    user_data = db.get_user(user.id)

    safe_first_name = escape_markdown_v2(user.first_name if user.first_name else "Пользователь")
    
    if user_data and user_data['is_authorized']:
        auth_status_text = "✅ Авторизован"
        if user.id in ADMIN_TELEGRAM_IDS: # Проверяем, является ли текущий пользователь админом из конфига
            # Дополнительно можно проверить user_data['is_admin'], если это поле используется и синхронизируется
            auth_status_text += " (Администратор)"
        
        text_to_send = (
            f"👋 С возвращением, {safe_first_name}\\!\n"
            f"Ваш Telegram ID: `{user.id}`\n"
            f"Статус: {escape_markdown_v2(auth_status_text)}\n\n"
            "Используйте меню команд \\(кнопка `/` или три полоски\\) для взаимодействия с ботом\\."
        )
        await update.message.reply_text(text_to_send, parse_mode=ParseMode.MARKDOWN_V2)
    elif user_data and user_data['application_status'] == 'pending':
        text_to_send = (
            f"👋 Привет, {safe_first_name}\\!\n"
            "Ваша заявка на доступ находится на рассмотрении администратором\\. Пожалуйста, ожидайте\\."
        )
        await update.message.reply_text(text_to_send, parse_mode=ParseMode.MARKDOWN_V2)
    else: 
        text_to_send = (
            f"👋 Привет, {safe_first_name}\\!\n"
            "Этот бот предназначен для сотрудников ОПК МТУСИ для учета посещаемости рабочего места\\.\n\n"
            "Если вы сотрудник, пожалуйста, подайте заявку на доступ к боту\\."
        )
        keyboard = [[InlineKeyboardButton("Подать заявку на доступ", callback_data="apply_for_access")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text_to_send, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)


async def on_shift_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Начинает диалог просмотра сотрудников на смене.
    Показывает кнопки с департаментами, существующими в базе.
    """
    logger.info(f"Админ {update.effective_user.id} вызвал команду /on_shift")
    
    # ИСПРАВЛЕНИЕ: Вызываем функцию с правильным именем 'get_unique_user_departments'
    departments = db.get_unique_user_departments()
    
    if not departments:
        await update.message.reply_text("В базе данных не найдено пользователей с указанными департаментами.")
        return


    keyboard = []
    for department_name in departments:
        keyboard.append([
            InlineKeyboardButton(f"Департамент: {department_name}", callback_data=f"on_shift_dept:{department_name}")
        ])
    
    keyboard.append([InlineKeyboardButton("Показать всех", callback_data="on_shift_dept:ALL")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="on_shift_cancel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите департамент для просмотра:", reply_markup=reply_markup)

# Этот обработчик будет ловить нажатия на кнопки с секторами
async def on_shift_button_press(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатывает нажатие на кнопку, форматирует вывод в виде таблицы
    с выравниванием по правому краю для времени.
    """
    query = update.callback_query
    await query.answer()

    try:
        command, _, department_choice = query.data.partition(':')

        if command == "on_shift_cancel":
            await query.edit_message_text("Действие отменено.")
            return
            
        logger.info(f"Админ {query.from_user.id} запросил список на смене для департамента: {department_choice}")

        active_users = db.get_active_users_by_department(department_choice)

        display_header = "Все" if department_choice == "ALL" else department_choice
        message_text = f"<b>👥 Сотрудники на смене (Департамент: {display_header})</b>\n"

        if not active_users:
            message_text += "\nНа смене никого нет."
            await query.edit_message_text(text=message_text, parse_mode=ParseMode.HTML)
            return

        formatted_lines = {}
        
        for user in active_users:
            # --- ГЛАВНОЕ ИЗМЕНЕНИЕ: ИСПОЛЬЗУЕМ application_full_name ---
            # Приоритет - полное имя. Если его нет, то никнейм.
            display_name = user['application_full_name'] or user['username'] or 'Без имени'

            try:
                checkin_dt = datetime.strptime(user['check_in_time'], '%Y-%m-%d %H:%M:%S')
                checkin_time_str = checkin_dt.strftime('%H:%M')
            except (ValueError, TypeError):
                checkin_time_str = "??:??"
            
            department = user['application_department'] or "Без департамента"
            
            if department not in formatted_lines:
                formatted_lines[department] = []
            formatted_lines[department].append({'name': display_name, 'time': checkin_time_str})

        max_name_length = 0
        for dept_lines in formatted_lines.values():
            for line in dept_lines:
                if len(line['name']) > max_name_length:
                    max_name_length = len(line['name'])
        
        for department, employees in sorted(formatted_lines.items()):
            message_text += f"\n<b>{department}:</b>\n"
            
            table_rows = []
            for emp in employees:
                aligned_name = emp['name'].ljust(max_name_length)
                table_rows.append(f"<code>{aligned_name}  |  {emp['time']}</code>")
            
            message_text += "\n".join(table_rows)
            
        await query.edit_message_text(text=message_text, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"Критическая ошибка в on_shift_button_press: {e}", exc_info=True)
        await query.edit_message_text("Произошла внутренняя ошибка при формировании списка. Пожалуйста, проверьте логи.")




async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text_to_send = (
        "📋 *Справка по боту:*\n"
        "Этот бот помогает вести учет рабочего времени сотрудников\\.\n\n"
        "Используйте кнопку меню \\(обычно `/` или три полоски\\) рядом с полем ввода, чтобы увидеть список доступных команд и их описания\\.\n\n"
        "Основные команды:\n"
        "`/start` \\- Начало работы и информация о статусе\n"
        "`/checkin` \\- Отметить приход на работу\n"
        "`/checkout` \\- Отметить уход с работы\n\n"
        "Если вы неавторизованный сотрудник, используйте команду `/start` для подачи заявки на доступ\\.\n\n"
        "Если вы администратор, вам также доступны специальные команды для управления пользователями и ботом\\."
    )
    try:
        await update.message.reply_text(text_to_send, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Ошибка при отправке help_command с MarkdownV2: {e}", exc_info=True)
        await update.message.reply_text("Не удалось отобразить справку с форматированием. Попробуйте позже.")



async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer() 
    user = query.from_user
    callback_data = query.data

    # Используйте тот уровень логирования, который вам нужен для отладки
    logger.info(f"--- button_callback_handler ВЫЗВАНА с data: {callback_data} для user: {user.id} ---")

    if callback_data == "apply_for_access":
        logger.info(f"User {user.id}: Обработка 'apply_for_access'.")

        logger.info(f"User {user.id}: ПЕРЕД вызовом db.get_user()")
        user_db_data = db.get_user(user.id) # Эта функция должна ТОЛЬКО ЧИТАТЬ данные
        logger.info(f"User {user.id}: ПОСЛЕ вызова db.get_user(). Результат: {user_db_data}")

        if user_db_data:
            try:
                is_authorized = user_db_data['is_authorized']
                # Предположим, что если is_authorized == 0 и есть application_full_name, то заявка в ожидании
                application_full_name_in_db = user_db_data['application_full_name'] 
            except KeyError as e_key:
                logger.error(f"User {user.id}: Ключ '{e_key}' не найден в user_db_data. Структура user_db_data: {dict(user_db_data) if user_db_data else None}")
                # Обработка ошибки - возможно, структура БД не та, что ожидается
                await query.edit_message_text("Произошла внутренняя ошибка при проверке вашего статуса. Пожалуйста, попробуйте позже.")
                return ConversationHandler.END

            if is_authorized == 1: # Пользователь уже авторизован
                logger.info(f"User {user.id}: Уже авторизован.")
                status_msg = "Вы уже авторизованы и можете пользоваться ботом."
                try:
                    await query.edit_message_text(text=status_msg)
                except Exception:
                    await context.bot.send_message(chat_id=user.id, text=status_msg)
                return ConversationHandler.END # Завершаем диалог

            # Проверяем application_status или эквивалентную логику
            # Если is_authorized == 0 (или False/NULL), то это может быть pending, rejected, или новая заявка.
            # В вашем коде вы проверяли user_data['application_status'] == 'pending'
            # Если у вас нет application_status, а только is_authorized и application_full_name:
            elif is_authorized == 0 and application_full_name_in_db: # Заявка уже подана и ожидает
                logger.info(f"User {user.id}: Заявка уже на рассмотрении (is_authorized=0, ФИО есть).")
                status_msg = "Ваша заявка уже находится на рассмотрении."
                try:
                    await query.edit_message_text(text=status_msg)
                except Exception:
                    await context.bot.send_message(chat_id=user.id, text=status_msg)
                return ConversationHandler.END 
            
        logger.info(f"User {user.id}: Условия для начала новой заявки выполнены. Запрос ФИО.")
        
        request_fio_text = "Пожалуйста, введите ваше полное ФИО (например, Иванов Иван Иванович):"
        try:
            await query.edit_message_text(
                text=escape_markdown_v2(request_fio_text),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение для 'apply_for_access' (запрос ФИО): {e}. Отправляю новое.")
            await context.bot.send_message(
                chat_id=user.id,
                text=escape_markdown_v2(request_fio_text),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        return ASK_FULL_NAME # Переходим в состояние запроса ФИО

    logger.warning(f"User {user.id}: Неожиданный callback_data '{callback_data}' в button_callback_handler для application_flow.")
    if query:
        try:
            await query.edit_message_text(text=f"Неизвестная команда кнопки: {escape_markdown_v2(callback_data)}")
        except Exception:
            pass 
    return ConversationHandler.END

async def receive_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет ФИО и предлагает выбрать сектор кнопками."""
    user_id = update.effective_user.id
    full_name = update.message.text.strip()

    if not full_name or len(full_name) < 5: 
        await update.message.reply_text("Пожалуйста, введите корректное полное имя (не менее 5 символов).")
        return ASK_FULL_NAME 
    context.user_data['application_full_name'] = full_name
    logger.info(f"User {user_id} ввел ФИО: {full_name} для заявки.")

    keyboard = []
    for sector_display_name in PREDEFINED_SECTORS: # e.g., "Сектор СС"
        # Извлекаем ключ сектора (например, "СС") для callback_data
        parts = sector_display_name.split()
        sector_key = parts[-1].upper() if len(parts) > 1 else sector_display_name.upper()
        keyboard.append([InlineKeyboardButton(sector_display_name, callback_data=f"reg_select_dept_{sector_key}")])
    
    # Добавляем кнопку отмены прямо в клавиатуру выбора сектора
    keyboard.append([InlineKeyboardButton("❌ Отменить заявку", callback_data="reg_cancel_direct")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"Отлично, {full_name}!\nШаг 2: Теперь выберите ваш сектор из списка:",
        reply_markup=reply_markup
    )
    return ASK_DEPARTMENT

async def receive_department(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    department = update.message.text
    if not department or len(department.strip()) < 2: 
        await update.message.reply_text(
            escape_markdown_v2("Пожалуйста, введите корректное название сектора (не менее 2 символов)."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ASK_DEPARTMENT 

    context.user_data['application_department'] = department.strip()
    logger.info(f"Пользователь {user.id} ввел Сектор: {department.strip()}")

    full_name = context.user_data.get('application_full_name')
    
    if not full_name: 
        logger.error(f"Отсутствует ФИО в context.user_data для пользователя {user.id} при подаче заявки.")
        await update.message.reply_text(escape_markdown_v2("Произошла ошибка, ФИО не было сохранено. Пожалуйста, начните заново с /start."), parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END

    # Вызываем db.submit_application для сохранения данных в БД
    success, message = db.submit_application(user.id, full_name, department.strip())
    
    # Отправляем сообщение пользователю о результате подачи заявки
    await update.message.reply_text(escape_markdown_v2(message), parse_mode=ParseMode.MARKDOWN_V2)

    if success and "успешно подана" in message:
        safe_display_username = escape_markdown_v2(user.username or "нет username")
        username_for_admin_msg = f"\\(@{safe_display_username}\\)" if user.username else escape_markdown_v2("(нет username)")

        # Формируем текст уведомления для администратора
        admin_text_message = (
            f"🔔 *Новая заявка на доступ\\!*\n\n"  
            f"Пользователь: {escape_markdown_v2(user.first_name or '')} {escape_markdown_v2(user.last_name or '')} "
            f"{username_for_admin_msg} ID: `{user.id}`\n"
            f"Указанное ФИО: {escape_markdown_v2(full_name)}\n"
            f"Указанный Сектор: {escape_markdown_v2(department.strip())}\n" 
        )
        
        keyboard_admin_notification = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📋 Посмотреть в списке (ID: {user.id})", callback_data=f"focus_in_list:{user.id}")]
        ])

        # ОСТАВЛЯЕМ ТОЛЬКО ОДИН ЦИКЛ С КОРРЕКТНЫМ TRY-EXCEPT
        for admin_id_loop in ADMIN_TELEGRAM_IDS: 
            try:
                await context.bot.send_message(
                    chat_id=admin_id_loop,
                    text=admin_text_message,
                    reply_markup=keyboard_admin_notification, 
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление администратору {admin_id_loop}: {e}", exc_info=True)
    
    # Очищаем user_data после завершения диалога
    context.user_data.pop('application_full_name', None)
    context.user_data.pop('application_department', None)
    return ConversationHandler.END 

async def process_department_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор сектора из кнопок и завершает подачу заявки."""
    query = update.callback_query
    await query.answer()
    selected_callback_data = query.data
    user = query.from_user

    logger.info(f"User {user.id} ({user.username}) в process_department_selection, выбрал callback: {selected_callback_data}")

    if selected_callback_data == "reg_cancel_direct":
        logger.info(f"User {user.id} отменил подачу заявки на этапе выбора сектора.")
        await query.edit_message_text("Подача заявки отменена.")
        context.user_data.clear()
        return ConversationHandler.END

    # Извлекаем ключ сектора из callback_data (например, "СС" из "reg_select_dept_СС")
    try:
        department_key = selected_callback_data.replace("reg_select_dept_", "")
    except AttributeError: # На случай, если selected_callback_data не строка
        logger.error(f"User {user.id}: Ошибка извлечения department_key из callback_data: {selected_callback_data}")
        await query.edit_message_text("Произошла ошибка при выборе сектора. Пожалуйста, попробуйте снова.")
        context.user_data.clear()
        return ConversationHandler.END
        
    # Получаем ФИО из user_data
    full_name = context.user_data.get('application_full_name')
    if not full_name:
        logger.error(f"User {user.id}: ФИО не найдено в user_data на этапе выбора департамента. Завершение диалога.")
        await query.edit_message_text("Произошла ошибка: не удалось найти ваше ФИО. Пожалуйста, начните подачу заявки заново командой /start.")
        context.user_data.clear()
        return ConversationHandler.END

    # Сохраняем выбранный департамент (ключ) в user_data (хотя он сразу используется для БД)
    context.user_data['application_department'] = department_key
    username = user.username if user.username else "N/A"
    
    logger.info(f"User {user.id}: Подготовка к сохранению заявки. ФИО: '{full_name}', Департамент (ключ): '{department_key}'")

    try:
        # Используем функцию db.submit_application из вашего database_operations.py
        success, message_from_db = db.submit_application(
            telegram_id=user.id,
            full_name=full_name,
            department=department_key
        )

        if success:
            logger.info(f"Заявка на доступ успешно подана/обновлена для ID {user.id}. Сообщение от БД: {message_from_db}")
            
            # Ищем полное отображаемое имя сектора для сообщения пользователю и администраторам
            selected_sector_display_name = department_key # По умолчанию, если не найдено полное имя
            for predefined_sector_full_name in PREDEFINED_SECTORS: # PREDEFINED_SECTORS должно быть определено
                parts = predefined_sector_full_name.split()
                # Предполагаем, что ключ - это последнее слово в названии сектора (например, "СС" из "Сектор СС")
                abbr = parts[-1].upper() if len(parts) > 1 else predefined_sector_full_name.upper()
                if abbr == department_key.upper(): # Сравнение в верхнем регистре для надежности
                    selected_sector_display_name = predefined_sector_full_name
                    break
            
            # Отправляем пользователю подтверждение (используем сообщение от db.submit_application)
            await query.edit_message_text(text=message_from_db) # message_from_db уже содержит "✅ Ваша заявка..."
            
            # Уведомление администраторам
            admin_message = (
                f"🔔 Новая заявка на доступ!\n"
                f"Пользователь: {full_name} (@{username if username != 'N/A' else user.id})\n"
                f"Телеграм ID: {user.id}\n"
                f"Выбранный сектор: {selected_sector_display_name} (ключ: {department_key})\n\n"
                f"Для просмотра ожидающих заявок: /admin_pending_users"
            )
            if ADMIN_TELEGRAM_IDS: 
                for admin_id in ADMIN_TELEGRAM_IDS:
                    try:
                        await context.bot.send_message(chat_id=admin_id, text=admin_message)
                    except Exception as e_admin:
                        logger.error(f"Не удалось отправить уведомление администратору {admin_id} о новой заявке от {user.id}: {e_admin}")
            else:
                logger.warning("Список ADMIN_IDS пуст, уведомления администраторам о новой заявке не отправлены.")
        
        else:
            # Если submit_application вернула False, значит была какая-то логическая ошибка
            # (например, заявка уже подана, или пользователь не найден в БД до обновления)
            # или ошибка БД, которая уже залогирована в submit_application.
            logger.warning(f"Не удалось подать заявку для пользователя {user.id} через db.submit_application. Сообщение от БД: {message_from_db}")
            await query.edit_message_text(text=message_from_db) # Показываем пользователю сообщение об ошибке от submit_application
            
    except AttributeError as e_attr:
        # Этот блок на случай, если db.submit_application все еще не найдена (хотя мы это исправили)
        logger.critical(f"Критическая ошибка: функция db.submit_application не найдена или неверно импортирована! Ошибка: {e_attr}", exc_info=True)
        await query.edit_message_text(
            text="Произошла критическая ошибка при обработке вашей заявки (код ошибки: DB_FUNC_MISSING). Пожалуйста, свяжитесь с администратором."
        )
    except Exception as e: 
        # Ловим другие непредвиденные ошибки на этом уровне
        logger.error(f"Непредвиденная ошибка в process_department_selection для пользователя {user.id}: {e}", exc_info=True)
        await query.edit_message_text(
            text="Произошла непредвиденная ошибка при обработке вашей заявки. Пожалуйста, попробуйте позже или свяжитесь с администратором."
        )
    finally:
        context.user_data.clear() # Очищаем данные диалога в любом случае
        
    return ConversationHandler.END


async def cancel_application_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    logger.info(f"Пользователь {user.id} отменил диалог подачи заявки.")
    await update.message.reply_text(
        escape_markdown_v2("Подача заявки отменена. Вы можете начать заново командой /start."),
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.MARKDOWN_V2
    )
    context.user_data.pop('application_full_name', None)
    context.user_data.pop('application_department', None)
    return ConversationHandler.END

async def checkin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_data_from_db = db.get_user(user.id)
    is_admin_from_config = user.id in ADMIN_TELEGRAM_IDS
    is_authorized_in_db = user_data_from_db and user_data_from_db['is_authorized']
    if not (is_admin_from_config or is_authorized_in_db):
        error_message = escape_markdown_v2("❌ Вы не авторизованы для отметки о приходе. Обратитесь к администратору или подайте заявку через /start.")
        await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        logger.warning(f"Неавторизованная попытка /checkin от {user.id} ({user.username})")
        return
    if is_admin_from_config and (not user_data_from_db or not is_authorized_in_db):
        db.add_or_update_user(user.id, user.username, user.first_name, user.last_name)
        db.authorize_user(user.id, user.id) # Админа авторизуем сразу
        logger.info(f"Администратор {user.id} ({user.username}) автоматически добавлен/авторизован при попытке /checkin.")
    
    location_button = KeyboardButton(text="📍 Отправить мою геолокацию", request_location=True)
    reply_markup = ReplyKeyboardMarkup([[location_button]], resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(
        escape_markdown_v2("Для отметки о приходе, пожалуйста, поделитесь вашей текущей геолокацией."),
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN_V2
    )

async def checkout_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    user_data_from_db = db.get_user(user.id)
    is_admin_from_config = user.id in ADMIN_TELEGRAM_IDS
    is_authorized_in_db = user_data_from_db and user_data_from_db['is_authorized']
    if not (is_admin_from_config or is_authorized_in_db):
        error_message = escape_markdown_v2("❌ Вы не авторизованы для использования этой команды. Обратитесь к администратору или подайте заявку через /start.")
        await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        logger.warning(f"Неавторизованная попытка /checkout от {user.id} ({user.username})")
        return
    if is_admin_from_config and (not user_data_from_db or not is_authorized_in_db):
        db.add_or_update_user(user.id, user.username, user.first_name, user.last_name)
        db.authorize_user(user.id, user.id)
        logger.info(f"Администратор {user.id} ({user.username}) автоматически добавлен/авторизован при попытке /checkout.")
    success, message_from_db = db.record_check_out(user.id)
    await update.message.reply_text(escape_markdown_v2(message_from_db), parse_mode=ParseMode.MARKDOWN_V2)

async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message # Сообщение, содержащее локацию

    # Проверка, есть ли вообще объект сообщения и локации
    if not message or not message.location:
        logger.warning(f"location_handler вызван без объекта location для пользователя {user.id} ({user.username})")
        # Можно отправить сообщение пользователю, если это неожиданное поведение
        # await update.message.reply_text("Произошла ошибка при получении вашей геолокации. Пожалуйста, попробуйте снова.")
        return

    location = message.location
    latitude = location.latitude
    longitude = location.longitude

    logger.info(f"Получена геолокация от {user.id} ({user.username}): Широта={latitude}, Долгота={longitude}")

    # 1. Проверка авторизации пользователя
    user_data_from_db = db.get_user(user.id)
    is_admin_from_config = user.id in ADMIN_TELEGRAM_IDS
    is_authorized_in_db = user_data_from_db and user_data_from_db['is_authorized']

    if not (is_admin_from_config or is_authorized_in_db):
        error_message_unauth = escape_markdown_v2("❌ Ошибка: Вы не авторизованы. Отметка не будет сохранена.")
        await message.reply_text(error_message_unauth, parse_mode=ParseMode.MARKDOWN_V2)
        logger.warning(f"Неавторизованная попытка отправки геолокации от {user.id} ({user.username})")
        await message.reply_text(
            escape_markdown_v2("Пожалуйста, обратитесь к администратору для авторизации или подайте заявку через /start."),
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    # (Опционально) Автоматическая авторизация админа, если он отправил локацию, но не был в БД/авторизован.
    if is_admin_from_config and (not user_data_from_db or not is_authorized_in_db):
        db.add_or_update_user(user.id, user.username, user.first_name, user.last_name) # [[3]]
        db.authorize_user(user.id, user.id) # Админа авторизуем сразу
        logger.info(f"Администратор {user.id} ({user.username}) автоматически добавлен/авторизован при отправке локации.")
        # После автоматической авторизации, можно продолжить обработку check-in
        # или попросить его отправить локацию еще раз, так как user_data_from_db мог быть None.
        # Для простоты, продолжим.

    # 2. Проверка нахождения в геозоне
    if is_within_office_zone(latitude, longitude):
        # Пользователь в зоне, производим check-in
        logger.info(f"Пользователь {user.id} находится в разрешенной геозоне. Попытка check-in.")
        success, message_from_db = db.record_check_in(user.id, latitude, longitude)
        
        # Отправляем результат операции check-in пользователю
        await message.reply_text(
            escape_markdown_v2(message_from_db), 
            reply_markup=ReplyKeyboardRemove(), 
            parse_mode=ParseMode.MARKDOWN_V2
        )
    else:
        # Пользователь вне зоны
        logger.info(f"Пользователь {user.id} находится вне разрешенной геозоны. Check-in отклонен.")
        error_message_geofence = escape_markdown_v2(
            "❌ Вы находитесь слишком далеко от офиса для отметки прихода. "
            "Пожалуйста, подойдите ближе и попробуйте снова, используя команду /checkin."
        )
        await message.reply_text(
            error_message_geofence, 
            reply_markup=ReplyKeyboardRemove(), 
            parse_mode=ParseMode.MARKDOWN_V2
        )

def is_within_office_zone(latitude: float, longitude: float) -> bool:
    """
    Проверяет, находятся ли предоставленные координаты внутри THE_OFFICE_ZONE.
    """
    # Сначала убедимся, что THE_OFFICE_ZONE определена и имеет нужные ключи
    if not THE_OFFICE_ZONE or not all(k in THE_OFFICE_ZONE for k in ["min_latitude", "max_latitude", "min_longitude", "max_longitude"]):
        logger.error("Конфигурация THE_OFFICE_ZONE не определена или неполная. Проверка геозоны невозможна.")
        return False 

    min_lat = THE_OFFICE_ZONE["min_latitude"]
    max_lat = THE_OFFICE_ZONE["max_latitude"]
    min_lon = THE_OFFICE_ZONE["min_longitude"]
    max_lon = THE_OFFICE_ZONE["max_longitude"]

    if min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon:
        logger.info(f"Координаты ({latitude}, {longitude}) находятся ВНУТРИ разрешенной зоны.")
        return True
    else:
        logger.info(f"Координаты ({latitude}, {longitude}) находятся ВНЕ разрешенной зоны.")
        return False
    
async def request_manual_checkin_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Начинает диалог запроса ручной отметки прихода.
    Запрашивает у пользователя фактическое время прихода.
    """
    user = update.effective_user
    logger.info(f"User {user.id} ({user.username}) инициировал запрос на ручную отметку прихода (/request_manual_checkin).")
    
    # Проверка, авторизован ли пользователь (если это необходимо для этой функции)
    # Если неавторизованные не могут подавать такие заявки, добавьте проверку:
    if not db.is_user_authorized(user.id):
        await update.message.reply_text("Эта функция доступна только для авторизованных пользователей.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Вы хотите запросить ручную отметку прихода.\n"
        "Пожалуйста, укажите фактическое время вашего прихода в формате **ДД.ММ.ГГГГ ЧЧ:ММ** "
        "(например, `15.06.2025 09:05`).\n\n"
        "Для отмены введите /cancel_manual_checkin"
    )
    return REQUEST_MANUAL_CHECKIN_TIME

async def process_manual_checkin_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает введенное пользователем время для ручной отметки прихода.
    Валидирует время и сохраняет заявку в БД.
    """
    user = update.effective_user
    user_input_time_str = update.message.text
    
    try:
        # Валидация и преобразование времени
        # Мы ожидаем формат "ДД.ММ.ГГГГ ЧЧ:ММ"
        requested_time_dt = datetime.strptime(user_input_time_str, '%d.%m.%Y %H:%M')
    
        success = db.add_manual_checkin_request(user_id=user.id, requested_checkin_time=requested_time_dt)
        
        if success:
            await update.message.reply_text(
                f"Спасибо! Ваша заявка на ручную отметку прихода на "
                f"**{requested_time_dt.strftime('%d.%m.%Y %H:%M')}** принята и отправлена администратору."
            )
            logger.info(f"User {user.id} успешно подал заявку на ручную отметку прихода на {requested_time_dt}.")
            
            # Уведомление администраторам о новой заявке
            # (Эту функцию нужно будет создать или использовать существующую, если есть)
            # await notify_admins_new_manual_request(context.bot, user, requested_time_dt)

        else:
            await update.message.reply_text(
                "Произошла ошибка при сохранении вашей заявки. Пожалуйста, попробуйте позже или свяжитесь с администратором."
            )
            logger.error(f"Не удалось сохранить заявку на ручную отметку для user {user.id} на время {requested_time_dt}.")
            
        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text(
            "Неверный формат времени. Пожалуйста, введите время в формате **ДД.ММ.ГГГГ ЧЧ:ММ** "
            "(например, `15.06.2025 09:05`).\n\n"
            "Для отмены введите /cancel_manual_checkin"
        )
        return REQUEST_MANUAL_CHECKIN_TIME # Остаемся в том же состоянии для повторного ввода
    except Exception as e:
        logger.error(f"Ошибка в process_manual_checkin_time для user {user.id}: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла непредвиденная ошибка. Пожалуйста, попробуйте позже."
        )
        return ConversationHandler.END

async def cancel_manual_checkin_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет диалог запроса ручной отметки."""
    user = update.effective_user
    await update.message.reply_text("Запрос на ручную отметку прихода отменен.")
    logger.info(f"User {user.id} отменил диалог запроса ручной отметки.")
    context.user_data.clear() # Очищаем user_data на всякий случай, если что-то там сохраняли
    return ConversationHandler.END

# --- Определение ConversationHandler для ручной отметки ---
manual_checkin_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("request_manual_checkin", request_manual_checkin_start)],
    states={
        REQUEST_MANUAL_CHECKIN_TIME: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, process_manual_checkin_time)
        ],
    },
    fallbacks=[CommandHandler("cancel_manual_checkin", cancel_manual_checkin_dialog)],
    name="manual_checkin_request_flow",
    per_user=True,
    per_chat=True
)


async def notify_admins_new_manual_request(bot: Bot, requesting_user: User, requested_time: datetime):
    """Уведомляет администраторов о новой заявке на ручную отметку прихода."""
    logger.info(f"Подготовка уведомления администраторам о заявке от user_id={requesting_user.id}")
    
    user_profile_row = db.get_user(requesting_user.id) # Используем вашу функцию из database_operations
    
    department_name = "Не указан"
    if user_profile_row and user_profile_row['application_department']:
        dept_key_from_db = user_profile_row['application_department']
        found_display_name = False
        # Ищем полное имя сектора в PREDEFINED_SECTORS по ключу из БД (например, "СС", "ВИ")
        for predefined_name in PREDEFINED_SECTORS: 
            # Предполагаем, что PREDEFINED_SECTORS - это список строк типа "Сектор СС", "Сектор ВИ"
            # И что ключ в БД dept_key_from_db - это "СС", "ВИ" и т.д.
            # Вам может понадобиться адаптировать эту логику под то, как у вас хранятся 
            # и соотносятся ключи секторов и их полные названия.
            # Пример: если predefined_name = "Сектор СС", а dept_key_from_db = "СС"
            if predefined_name.endswith(dept_key_from_db): # Простая проверка по окончанию
                department_name = predefined_name
                found_display_name = True
                break
        if not found_display_name:
            department_name = dept_key_from_db # Если не нашли, используем ключ как есть
            logger.warning(f"Для ключа сектора '{dept_key_from_db}' пользователя {requesting_user.id} не найдено отображаемое имя в PREDEFINED_SECTORS.")
    
    username_escaped = escape_markdown_v2(requesting_user.username or "N/A")
    full_name_escaped = escape_markdown_v2(requesting_user.full_name)
    department_name_escaped = escape_markdown_v2(department_name)
    # Форматируем время для отображения, экранируем его для MarkdownV2
    requested_time_str_display = requested_time.strftime('%d.%m.%Y %H:%M')
    requested_time_str_escaped = escape_markdown_v2(requested_time_str_display)

    message_text = (
        f"‼️ Новая заявка на ручную отметку прихода\\!\n\n"
        f"👤 **Пользователь:** {full_name_escaped} (@{username_escaped})\n"
        f"🆔 **Telegram ID:** `{requesting_user.id}`\n"
        f"🏢 **Зарегистрированный сектор:** {department_name_escaped}\n"
        f"⏰ **Запрошенное время прихода:** *{requested_time_str_escaped}*\n\n"
        f"Для обработки заявок используйте команду `/admin_manual_checkins` (будет реализована позже)\\."
    )
    
    if not ADMIN_TELEGRAM_IDS:
        logger.warning("Список ADMIN_TELEGRAM_IDS пуст. Уведомления о ручных заявках не будут отправлены.")
        return

    for admin_id in ADMIN_TELEGRAM_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id, 
                text=message_text, 
                parse_mode=ParseMode.MARKDOWN_V2
            )
            logger.info(f"Уведомление о ручной заявке от user_id={requesting_user.id} отправлено админу {admin_id}.")
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление о ручной заявке админу {admin_id}: {e}", exc_info=True)


async def admin_manual_checkins_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    admin_user = update.effective_user
    logger.info(f"Вызвана функция admin_manual_checkins_start админом {admin_user.id}")
    
    if not is_admin(admin_user.id):
        await update.message.reply_text("Эта команда доступна только для администраторов.")
        return ConversationHandler.END

    logger.info(f"Администратор {admin_user.id} ({admin_user.username}) запросил список ручных заявок (/admin_manual_checkins).")

    pending_requests = db.get_pending_manual_checkin_requests()
    
    if not pending_requests:
        await update.message.reply_text("На данный момент нет ожидающих заявок на ручную отметку.")
        return ConversationHandler.END

    message_text = "<b>Ожидающие заявки на ручную отметку:</b>\n\n"
    keyboard = []

    for i, req in enumerate(pending_requests):
        try:
            req_time_utc = datetime.strptime(req['requested_checkin_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc)
            req_time_local = req_time_utc.astimezone(MOSCOW_TZ)
            formatted_time = req_time_local.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            formatted_time = req['requested_checkin_time']
        
        department = req['application_department'] or 'Не указан'
        user_info = f"{req['first_name'] or ''} {req['last_name'] or ''} (@{req['username'] or 'N/A'})"
        
        message_text += f"<b>{i+1}. {user_info}</b>\n"
        message_text += f"   - <b>Сектор:</b> {department}\n"
        message_text += f"   - <b>Время:</b> {formatted_time}\n\n"

        keyboard.append([
            InlineKeyboardButton(f"Рассмотреть заявку №{i+1}", callback_data=f"admin_process_req_{req['request_id']}")
        ])

    keyboard.append([InlineKeyboardButton("✅ Принять все", callback_data="admin_approve_all")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="admin_cancel_manual_dialog")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    
    return ADMIN_LIST_MANUAL_REQUESTS



async def approve_all_requests_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает нажатие на кнопку "Принять все".
    """
    query = update.callback_query
    await query.answer() # Убираем "часики"


    admin_user = query.from_user
    logger.info(f"Админ {admin_user.id} нажал на кнопку 'Принять все'.")


    # Вызываем нашу новую функцию из database.py
    approved_count, failed_count = db.approve_all_pending_manual_checkins()


    if approved_count == 0 and failed_count == 0:
        response_text = "Не найдено заявок для обработки."
    else:
        response_text = f"✅ Массовое одобрение завершено!\n\n" \
                        f"Успешно обработано: {approved_count} шт.\n" \
                        f"Пропущено из-за ошибок: {failed_count} шт."


    # Редактируем исходное сообщение, убирая клавиатуру
    await query.edit_message_text(text=response_text)


    # Завершаем диалог
    return ConversationHandler.END


async def admin_select_manual_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор конкретной заявки администратором.
    Показывает детали заявки и кнопки для действий.
    """
    query = update.callback_query
    await query.answer() 

    request_id_str = query.data.split('_')[-1]
    try:
        request_id = int(request_id_str)
    except ValueError:
        logger.error(f"Ошибка парсинга request_id из callback_data: {query.data}")
        await query.edit_message_text(
            text="Произошла ошибка при обработке вашего выбора\\.\nПожалуйста, попробуйте снова из списка заявок\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        return ConversationHandler.END

    logger.info(f"Администратор {query.from_user.id} выбрал для рассмотрения заявку ID: {request_id}.")
    
    request_data = db.get_manual_checkin_request_by_id(request_id) # request_data это sqlite3.Row

    if not request_data:
        await query.edit_message_text(
            text=f"Заявка с ID {request_id} не найдена\\.\nВозможно, она была удалена или уже обработана\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        return ADMIN_LIST_MANUAL_REQUESTS

    if request_data['status'] != 'pending':
        await query.edit_message_text(
            text=f"Заявка ID {request_id} уже была обработана ранее \\(статус: {escape_markdown_v2(str(request_data['status']))}\\)\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        return ADMIN_LIST_MANUAL_REQUESTS

    # Безопасное извлечение данных из sqlite3.Row
    req_username = request_data['username'] if 'username' in request_data.keys() else None
    req_first_name = request_data['first_name'] if 'first_name' in request_data.keys() else None
    req_last_name = request_data['last_name'] if 'last_name' in request_data.keys() else None

    context.user_data['current_manual_request'] = {
        'id': request_data['request_id'], # Предполагаем, что эти поля всегда есть
        'user_id': request_data['user_id'],
        'username': req_username,
        'first_name': req_first_name,
        'last_name': req_last_name,
        'application_department': request_data['application_department'],
        'requested_checkin_time_str': request_data['requested_checkin_time']
    }
    
    user_full_name = f"{req_first_name or ''} {req_last_name or ''}".strip()
    
    if user_full_name:
        user_display = user_full_name
        if req_username:
            user_display += f" (@{req_username})"
    elif req_username:
        user_display = f"@{req_username}"
    else:
        user_display = f"ID: {request_data['user_id']}" # user_id должен быть всегда
    
    escaped_user_display = escape_markdown_v2(user_display)

    try:
        requested_time_dt = datetime.strptime(request_data['requested_checkin_time'], '%Y-%m-%d %H:%M:%S')
        requested_time_display = requested_time_dt.strftime('%d.%m.%Y %H:%M')
    except (ValueError, TypeError):
        requested_time_display = request_data['requested_checkin_time']
    escaped_requested_time_display = escape_markdown_v2(requested_time_display)

    department_key = request_data['application_department']
    # department_display = SECTOR_MAPPING.get(department_key, department_key) 
    department_display = department_key
    escaped_department_display = escape_markdown_v2(department_display)

    message_text = (
        f"📄 **Рассмотрение заявки ID: {request_data['request_id']}**\n\n"
        f"👤 **Пользователь:** {escaped_user_display}\n"
        f"   \\(Telegram ID: `{request_data['user_id']}`\\)\n"
        f"🏢 **Сектор:** {escaped_department_display}\n"
        f"⏰ **Запрошенное время прихода:** *{escaped_requested_time_display}*\n\n"
        f"Выберите действие:"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Одобрить как есть", callback_data=f"admin_req_approve_as_is_{request_id}"),
            InlineKeyboardButton("✏️ Одобрить с другим временем", callback_data=f"admin_req_approve_new_time_{request_id}")
        ],
        [
            InlineKeyboardButton("❌ Отклонить заявку", callback_data=f"admin_req_reject_{request_id}")
        ],
        [
            InlineKeyboardButton("⬅️ Назад к списку заявок", callback_data="admin_req_back_to_list")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    except telegram.error.BadRequest as e: 
        logger.error(f"Ошибка BadRequest при отображении деталей заявки {request_id}: {e}. Текст: {message_text}")
        await context.bot.send_message(
            chat_id=query.from_user.id, 
            text="Не удалось отобразить детали заявки с форматированием из\\-за ошибки\\.\n"
                 "Пожалуйста, попробуйте снова или обратитесь к разработчику, если проблема повторяется\\.\n"
                 f"Ошибка: `{escape_markdown_v2(str(e))}`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ADMIN_LIST_MANUAL_REQUESTS 
    except Exception as e: 
        logger.error(f"Не удалось изменить/отправить сообщение для заявки {request_id}: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="Произошла непредвиденная ошибка при отображении деталей заявки\\."
        )
        return ADMIN_LIST_MANUAL_REQUESTS
        
    return ADMIN_PROCESS_SINGLE_REQUEST




async def admin_process_request_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    action_data = query.data 
    user_request_data = context.user_data.get('current_manual_request')

    if not user_request_data and not action_data.startswith("admin_req_back_to_list"):
        logger.warning(f"В admin_process_request_action не найдены данные о заявке в user_data. Callback: {action_data}")
        await query.edit_message_text(
            text="Произошла ошибка: данные о текущей заявке не найдены\\.\n"
                 "Пожалуйста, вернитесь к списку заявок и попробуйте снова командой /admin_manual_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None 
        )
        context.user_data.clear() 
        return ConversationHandler.END

    if action_data.startswith("admin_req_approve_as_is_"):
        context.user_data['admin_action'] = 'approve_as_is'
        if not user_request_data: 
            logger.error("user_request_data is None в admin_req_approve_as_is_")
            await query.edit_message_text("Критическая ошибка: данные заявки отсутствуют\\. Пожалуйста, начните сначала\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            context.user_data.clear()
            return ConversationHandler.END
            
        requested_time_str = user_request_data.get('requested_checkin_time_str')
        if not requested_time_str:
            logger.error(f"Отсутствует requested_checkin_time_str для заявки ID {user_request_data.get('id', 'N/A')}")
            await query.edit_message_text("Ошибка: не найдено запрошенное время в данных заявки\\. Пожалуйста, начните сначала\\.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            context.user_data.clear()
            return ConversationHandler.END

        try:
            final_time_dt = datetime.strptime(requested_time_str, '%Y-%m-%d %H:%M:%S')
            context.user_data['final_checkin_time_dt'] = final_time_dt
            return await admin_confirm_decision_prompt(update, context, "одобрить эту заявку (время как запрошено)")
        except ValueError:
            logger.error(f"Ошибка парсинга requested_checkin_time_str: {requested_time_str} для заявки ID {user_request_data.get('id', 'N/A')}")
            await query.edit_message_text(
                text="Ошибка в данных времени заявки\\.\n"
                     "Пожалуйста, вернитесь к списку и попробуйте снова командой /admin_manual_checkins\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=None
            )
            context.user_data.clear()
            return ConversationHandler.END

    elif action_data.startswith("admin_req_approve_new_time_"):
        context.user_data['admin_action'] = 'approve_new_time'
        
        # Возвращаем информативное сообщение, разделяя его на два для безопасности
        
        message_part1 = (
            "Пожалуйста, введите новое время прихода для этой заявки в формате **ДД\\.ММ\\.ГГГГ ЧЧ:ММ** "
            "\\(например, 15\\.06\\.2025 09:00\\)\\." # Убрали \n\n здесь
        )
        message_part2 = "Для отмены введите /admin_cancel_manual_checkins" # Без Markdown, просто текст
        
        logger.info(f"ADMIN_PROCESS_REQUEST_ACTION: Попытка отправить сообщение из ДВУХ ЧАСТЕЙ.")
        logger.info(f"Часть 1 (длина {len(message_part1)}): '{message_part1}'")
        logger.info(f"Часть 2 (длина {len(message_part2)}): '{message_part2}'")
        
        try:
            # Пытаемся удалить предыдущее сообщение
            if query.message:
                try:
                    await query.message.delete()
                    logger.info("ADMIN_PROCESS_REQUEST_ACTION: Предыдущее сообщение удалено.")
                except Exception as del_e:
                    logger.warning(f"ADMIN_PROCESS_REQUEST_ACTION: Не удалось удалить старое сообщение: {del_e}")

            # Отправляем первую часть с MarkdownV2
            await context.bot.send_message(
                chat_id=query.from_user.id, 
                text=message_part1,
                parse_mode=ParseMode.MARKDOWN_V2 
            )
            logger.info("ADMIN_PROCESS_REQUEST_ACTION: Часть 1 успешно отправлена.")

            # Отправляем вторую часть как простой текст (parse_mode=None по умолчанию)
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text=message_part2
            )
            logger.info("ADMIN_PROCESS_REQUEST_ACTION: Часть 2 успешно отправлена.")

        except telegram.error.BadRequest as e: 
            logger.error(f"ADMIN_PROCESS_REQUEST_ACTION: Ошибка BadRequest при отправке сообщения из ДВУХ ЧАСТЕЙ (вероятно, в части 1): {e}.", exc_info=True)
            logger.error(f"Текст части 1 был: '{message_part1}'")
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text="Ошибка форматирования. Введите время: ДД.ММ.ГГГГ ЧЧ:ММ (15.06.2025 09:00). Отмена: /admin_cancel_manual_checkins",
                parse_mode=None 
            )
        except Exception as e:
            logger.error(f"ADMIN_PROCESS_REQUEST_ACTION: Другая ошибка при отправке сообщения из ДВУХ ЧАСТЕЙ: {e}.", exc_info=True)
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text="Ошибка. Введите время: ДД.ММ.ГГГГ ЧЧ:ММ (15.06.2025 09:00). Отмена: /admin_cancel_manual_checkins",
                parse_mode=None
            )
            
        return ADMIN_ENTER_NEW_TIME

    elif action_data.startswith("admin_req_reject_"):
        context.user_data['admin_action'] = 'reject'
        return await admin_confirm_decision_prompt(update, context, "отклонить эту заявку")

    elif action_data == "admin_req_back_to_list":
        try:
            await query.delete_message()
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение при возврате к списку: {e}")
        
        keys_to_clear = ['current_manual_request', 'admin_action', 'final_checkin_time_dt']
        for key in keys_to_clear:
            if key in context.user_data:
                del context.user_data[key]
        
        return await admin_manual_checkins_start(update, context) 

    else:
        logger.warning(f"Неизвестное действие в admin_process_request_action: {action_data}")
        await query.edit_message_text(
            text="Неизвестное действие\\.\n"
                 "Возврат к списку заявок командой /admin_manual_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        context.user_data.clear()
        return ConversationHandler.END


async def admin_confirm_decision_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, action_description: str) -> int:
    query = update.callback_query 
    user_request_data = context.user_data.get('current_manual_request')
    admin_action = context.user_data.get('admin_action')

    if not user_request_data or not admin_action:
        error_message = "Ошибка: данные для подтверждения не найдены\\.\nПожалуйста, начните сначала командой /admin_manual_checkins\\." 
        if query:
            await query.edit_message_text(text=error_message, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
        elif hasattr(update, 'message') and update.message: 
            await update.message.reply_text(text=error_message, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
        else: 
             await context.bot.send_message(chat_id=update.effective_chat.id, text=error_message, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
        context.user_data.clear()
        return ConversationHandler.END # Завершаем, так как состояние потеряно

    user_full_name = f"{user_request_data.get('first_name', '') or ''} {user_request_data.get('last_name', '') or ''}".strip()
    # Формируем user_display более аккуратно
    if user_full_name:
        user_display = user_full_name
        if user_request_data.get('username'):
            user_display += f" (@{user_request_data.get('username')})"
    elif user_request_data.get('username'):
        user_display = f"@{user_request_data.get('username')}"
    else:
        user_display = f"ID: {user_request_data.get('user_id', 'N/A')}"

    # Экранируем action_description перед использованием
    escaped_action_description = escape_markdown_v2(action_description)
    
    confirm_message = f"❓ Вы уверены, что хотите {escaped_action_description} для пользователя {escape_markdown_v2(user_display)} \\(Заявка ID: {user_request_data['id']}\\)\\?" # Экранирован ?
    
    final_time_dt_to_display = None
    if admin_action == 'approve_as_is':
        final_time_dt_to_display = context.user_data.get('final_checkin_time_dt')
        if final_time_dt_to_display: # Добавляем информацию о времени, если оно есть
             confirm_message += f"\nВремя прихода: *{escape_markdown_v2(final_time_dt_to_display.strftime('%d.%m.%Y %H:%M'))}*"

    elif admin_action == 'approve_new_time': 
        final_time_dt_to_display = context.user_data.get('final_checkin_time_dt')
        if final_time_dt_to_display: 
             # Переопределяем confirm_message для этого случая, чтобы было понятнее
             confirm_message = (
                f"❓ Вы уверены, что хотите одобрить заявку ID {user_request_data['id']} для {escape_markdown_v2(user_display)} "
                f"с новым временем прихода: *{escape_markdown_v2(final_time_dt_to_display.strftime('%d.%m.%Y %H:%M'))}*\\?" # Экранирован ?
            )
        else: # Если вдруг время не передалось, хотя должно быть
            confirm_message = f"❓ Вы уверены, что хотите {escaped_action_description} \\(новое время не указано\\) для пользователя {escape_markdown_v2(user_display)} \\(Заявка ID: {user_request_data['id']}\\)\\?"

    # Этот блок был лишним, так как информация о времени уже добавлена выше для approve_as_is
    # if final_time_dt_to_display and admin_action == 'approve_as_is': 
    #     confirm_message += f"\nВремя прихода: *{escape_markdown_v2(final_time_dt_to_display.strftime('%d.%m.%Y %H:%M'))}*"

    keyboard = [
        [
            InlineKeyboardButton("✅ Да, подтвердить", callback_data="admin_confirm_final_yes"),
            InlineKeyboardButton("❌ Нет, назад", callback_data="admin_confirm_final_no_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query: 
        await query.edit_message_text(text=confirm_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    elif hasattr(update, 'message') and update.message: 
        # Это случай, когда мы пришли сюда после ввода нового времени (текстовое сообщение)
        await update.message.reply_text(text=confirm_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    else: 
        # Резервный вариант, если нет ни query, ни message (маловероятно)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=confirm_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)

    return ADMIN_CONFIRM_REQUEST_DECISION


async def admin_receive_new_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает введенное администратором новое время для заявки.
    """
    user_input_time_str = update.message.text
    user_request_data = context.user_data.get('current_manual_request')
    
    if not user_request_data:
        logger.warning("В admin_receive_new_time не найдены данные о заявке в user_data.")
        # Сообщение об ошибке с экранированием
        await update.message.reply_text(
            text="Произошла ошибка: данные о заявке не найдены\\.\n"
                 "Попробуйте начать сначала с /admin_manual_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2 # Добавляем parse_mode
        )
        context.user_data.clear()
        return ConversationHandler.END 

    try:
        # Валидация и преобразование времени от админа
        new_time_dt = datetime.strptime(user_input_time_str, '%d.%m.%Y %H:%M')
        
        context.user_data['final_checkin_time_dt'] = new_time_dt
        context.user_data['admin_action'] = 'approve_new_time'

        logger.info(f"Администратор {update.effective_user.id} ввел новое время {new_time_dt.strftime('%d.%m.%Y %H:%M')} для заявки ID {user_request_data.get('id', 'N/A')}.") # Используем .get для id

        # admin_confirm_decision_prompt уже должен корректно экранировать action_description
        # и формировать сообщение. Строка с новым временем также будет экранирована внутри него.
        return await admin_confirm_decision_prompt(update, context, f"одобрить эту заявку с новым временем {new_time_dt.strftime('%d.%m.%Y %H:%M')}")

    except ValueError:
        # Сообщение об ошибке формата времени с корректным экранированием
        await update.message.reply_text(
            "❌ Неверный формат времени\\.\n" # Экранирована точка
            "Пожалуйста, введите время в формате **ДД\\.ММ\\.ГГГГ ЧЧ:ММ** " # Экранированы точки
            "\\(например, `15\\.06\\.2025 09:05`\\)\\.\n\n" # Экранированы скобки и точки
            "Для отмены введите /admin_cancel_manual_checkins",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ADMIN_ENTER_NEW_TIME 
    except Exception as e:
        logger.error(f"Ошибка в admin_receive_new_time для заявки {user_request_data.get('id', 'N/A')}: {e}", exc_info=True)
        # Сообщение о непредвиденной ошибке с экранированием
        await update.message.reply_text(
            text="Произошла непредвиденная ошибка при обработке времени\\.\n"
                 "Попробуйте начать сначала с /admin_manual_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2 # Добавляем parse_mode
        )
        context.user_data.clear()
        return ConversationHandler.END

    

async def admin_handle_final_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает финальное подтверждение администратора (Да/Нет)
    по одобрению или отклонению заявки.
    """
    query = update.callback_query
    await query.answer()

    user_request_data = context.user_data.get('current_manual_request')
    admin_action = context.user_data.get('admin_action')
    final_checkin_time_dt = context.user_data.get('final_checkin_time_dt') 
    admin_id = query.from_user.id

    if not user_request_data or not admin_action:
        logger.warning("В admin_handle_final_confirmation не найдены данные о заявке или действии в user_data.")
        await query.edit_message_text(
            text="Произошла ошибка: данные для обработки не найдены\\.\n"
                 "Попробуйте начать сначала с /admin_manual_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        context.user_data.clear()
        return ConversationHandler.END

    request_id = user_request_data['id']
    user_id_for_request = user_request_data['user_id']
    user_sector_key = user_request_data['application_department']
    
    user_first_name = user_request_data.get('first_name', '')
    user_last_name = user_request_data.get('last_name', '')
    user_username = user_request_data.get('username')
    
    user_display_name_parts = []
    if user_first_name: user_display_name_parts.append(user_first_name)
    if user_last_name: user_display_name_parts.append(user_last_name)
    
    user_display_name = " ".join(user_display_name_parts).strip()
    if not user_display_name and user_username:
        user_display_name = f"@{user_username}"
    elif not user_display_name:
        user_display_name = f"ID: {user_id_for_request}"
    elif user_username:
        user_display_name += f" (@{user_username})"

    if query.data == "admin_confirm_final_yes":
        success = False
        action_performed_message = ""
        user_notification_message = ""
        escaped_user_display_name = escape_markdown_v2(user_display_name) # Экранируем один раз

        if admin_action == 'approve_as_is' or admin_action == 'approve_new_time':
            if not final_checkin_time_dt:
                logger.error(f"Ошибка: final_checkin_time_dt отсутствует для одобрения заявки {request_id}.")
                await query.edit_message_text(
                    text="Критическая ошибка: время для одобрения не найдено\\.\nОбратитесь к разработчику\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=None
                )
                context.user_data.clear()
                return ConversationHandler.END
            
            success = db.approve_manual_checkin_request(
                request_id=request_id,
                admin_id=admin_id,
                final_checkin_time_local=final_checkin_time_dt, 
                user_id=user_id_for_request,
                user_sector_key=user_sector_key
            )
            if success:
                time_str_display = final_checkin_time_dt.strftime('%d.%m.%Y %H:%M')
                escaped_time_str_display = escape_markdown_v2(time_str_display) # Экранируем время

                action_performed_message = f"✅ Заявка ID {request_id} для {escaped_user_display_name} успешно одобрена\\.\nВремя прихода: *{escaped_time_str_display}*\\."
                user_notification_message = (
                    f"✅ Ваша заявка на ручную отметку прихода \\(ID: {request_id}\\) была одобрена администратором\\.\n"
                    f"Установленное время прихода: **{escaped_time_str_display}**\\."
                )
                logger.info(f"Заявка {request_id} одобрена админом {admin_id}. Время: {time_str_display}")
            else:
                action_performed_message = f"⚠️ Не удалось одобрить заявку ID {request_id}\\.\nВозможно, она была обработана ранее или произошла ошибка БД\\."
                logger.error(f"Ошибка БД при одобрении заявки {request_id} админом {admin_id}.")
        
        elif admin_action == 'reject':
            success = db.reject_manual_checkin_request(request_id=request_id, admin_id=admin_id)
            if success:
                action_performed_message = f"❌ Заявка ID {request_id} для {escaped_user_display_name} успешно отклонена\\."
                user_notification_message = f"❌ Ваша заявка на ручную отметку прихода \\(ID: {request_id}\\) была отклонена администратором\\."
                logger.info(f"Заявка {request_id} отклонена админом {admin_id}.")
            else:
                action_performed_message = f"⚠️ Не удалось отклонить заявку ID {request_id}\\.\nВозможно, она была обработана ранее или произошла ошибка БД\\."
                logger.error(f"Ошибка БД при отклонении заявки {request_id} админом {admin_id}.")
        
        else:
            action_performed_message = "Неизвестное подтвержденное действие\\." # Экранирована точка
            logger.error(f"Неизвестный admin_action '{admin_action}' при подтверждении для заявки {request_id}")

        await query.edit_message_text(text=action_performed_message, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
        
        if user_notification_message:
            try:
                await context.bot.send_message(chat_id=user_id_for_request, text=user_notification_message, parse_mode=ParseMode.MARKDOWN_V2)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {user_id_for_request} о решении по заявке {request_id}: {e}")
        
        context.user_data.clear() 
        
        await context.bot.send_message(
            chat_id=admin_id, 
            text="Обработка завершена\\.\nЧтобы просмотреть другие заявки, введите /admin\\_manual\\_checkins\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END

    elif query.data == "admin_confirm_final_no_back":
        user_first_name_b = user_request_data.get('first_name', '')
        user_last_name_b = user_request_data.get('last_name', '')
        user_username_b = user_request_data.get('username')
        user_id_b = user_request_data.get('user_id')

        user_display_b_parts = []
        if user_first_name_b: user_display_b_parts.append(user_first_name_b)
        if user_last_name_b: user_display_b_parts.append(user_last_name_b)
        user_display_b = " ".join(user_display_b_parts).strip()
        if not user_display_b and user_username_b: user_display_b = f"@{user_username_b}"
        elif not user_display_b: user_display_b = f"ID: {user_id_b}"
        elif user_username_b: user_display_b += f" (@{user_username_b})"

        try:
            requested_time_dt = datetime.strptime(user_request_data['requested_checkin_time_str'], '%Y-%m-%d %H:%M:%S')
            requested_time_display = requested_time_dt.strftime('%d.%m.%Y %H:%M')
        except (ValueError, TypeError):
            requested_time_display = user_request_data['requested_checkin_time_str']
        
        department_display = user_request_data['application_department']
        # dept_key = user_request_data['application_department']
        # department_display = SECTOR_MAPPING.get(dept_key, dept_key) 

        message_text = (
            f"📄 **Рассмотрение заявки ID: {user_request_data['id']}**\n\n"
            f"👤 **Пользователь:** {escape_markdown_v2(user_display_b)}\n"
            f"   \\(Telegram ID: `{user_request_data['user_id']}`\\)\n"
            f"🏢 **Сектор:** {escape_markdown_v2(department_display)}\n"
            f"⏰ **Запрошенное время прихода:** *{escape_markdown_v2(requested_time_display)}*\n\n"
            f"Выберите действие:"
        )
        keyboard = [
            [
                InlineKeyboardButton("✅ Одобрить как есть", callback_data=f"admin_req_approve_as_is_{request_id}"),
                InlineKeyboardButton("✏️ Одобрить с другим временем", callback_data=f"admin_req_approve_new_time_{request_id}")
            ],
            [
                InlineKeyboardButton("❌ Отклонить заявку", callback_data=f"admin_req_reject_{request_id}")
            ],
            [
                InlineKeyboardButton("⬅️ Назад к списку заявок", callback_data="admin_req_back_to_list")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text=message_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        return ADMIN_PROCESS_SINGLE_REQUEST

    else:
        logger.warning(f"Неизвестное действие в admin_handle_final_confirmation: {query.data}")
        await query.edit_message_text(
            text="Неизвестное действие\\.", # Экранирована точка
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=None
        )
        context.user_data.clear()
        return ConversationHandler.END
    

async def admin_cancel_manual_checkins_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет диалог обработки ручных заявок администратором."""
    user = update.effective_user
    message_text = "Диалог обработки ручных заявок отменен."
    
    # Если отмена пришла через callback_query (нажатие кнопки "Отмена")
    query = update.callback_query
    if query:
        await query.answer()
        try:
            await query.edit_message_text(text=message_text + "\nЧтобы начать заново, введите /admin_manual_checkins.", reply_markup=None)
        except Exception as e: # Если сообщение не может быть отредактировано (например, слишком старое)
            logger.warning(f"Не удалось отредактировать сообщение при отмене админ диалога: {e}")
            await context.bot.send_message(chat_id=query.from_user.id, text=message_text + "\nЧтобы начать заново, введите /admin_manual_checkins.")
    # Если отмена пришла через команду /admin_cancel_manual_checkins
    elif update.message:
        await update.message.reply_text(message_text + "\nЧтобы начать заново, введите /admin_manual_checkins.")
    
    logger.info(f"Администратор {user.id} отменил диалог обработки ручных заявок.")
    context.user_data.clear() # Очищаем user_data от данных этого диалога
    return ConversationHandler.END


async def ask_export_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Отправляет сообщение с кнопками выбора периода для отчета.
    """
    query = update.callback_query # Эта функция чаще всего будет вызываться по callback
    
    text = "Шаг 2: Выберите период для отчета:"
    keyboard = [
        [InlineKeyboardButton("Сегодня", callback_data="export_period_today")],
        [InlineKeyboardButton("Вчера", callback_data="export_period_yesterday")],
        [InlineKeyboardButton("Эта неделя", callback_data="export_period_this_week")],
        [InlineKeyboardButton("Прошлая неделя", callback_data="export_period_last_week")],
        [InlineKeyboardButton("Этот месяц", callback_data="export_period_this_month")],
        [InlineKeyboardButton("Прошлый месяц", callback_data="export_period_last_month")],
        [InlineKeyboardButton("🗓️ Произвольный период", callback_data="export_period_custom")],
        [InlineKeyboardButton("⬅️ Назад (к выбору сектора)", callback_data="export_back_to_sector_selection")],
        [InlineKeyboardButton("❌ Отменить экспорт", callback_data="export_cancel_dialog")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query: 
        await query.answer()
        try:
            await query.edit_message_text(text=text, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Ошибка при редактировании сообщения в ask_export_period: {e}. Отправка нового сообщения.")
            if update.effective_chat:
                 await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup)
    elif update.message: 
         await update.message.reply_text(text=text, reply_markup=reply_markup)
    else:
        logger.error("ask_export_period вызвана без query и без message. Это неожиданно.")
        if update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка отображения выбора периода.")
        return ConversationHandler.END
        
    return SELECT_PERIOD


async def show_export_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Показывает выбранные параметры (сектор, период) и запрашивает подтверждение экспорта.
    """
    query = update.callback_query 
    
    sector_display_name = context.user_data.get('export_sector_display_name', 'Не выбран')
    period_display_text = context.user_data.get('export_period_display_text', 'Не выбран')
    
    start_date_obj = context.user_data.get('export_start_date') 
    end_date_obj = context.user_data.get('export_end_date')     

    start_dt_str = ""
    if isinstance(start_date_obj, (datetime, date)):
        start_dt_str = start_date_obj.strftime('%d.%m.%Y')
        if isinstance(start_date_obj, datetime):
             start_dt_str += start_date_obj.strftime(' %H:%M')
    else:
        logger.warning(f"start_date_obj в show_export_confirmation имеет неожиданный тип: {type(start_date_obj)} или отсутствует.")
        start_dt_str = "Неизвестно"

    end_dt_str = ""
    if isinstance(end_date_obj, (datetime, date)):
        end_dt_str = end_date_obj.strftime('%d.%m.%Y')
        if isinstance(end_date_obj, datetime):
            end_dt_str += end_date_obj.strftime(' %H:%M')
    else:
        logger.warning(f"end_date_obj в show_export_confirmation имеет неожиданный тип: {type(end_date_obj)} или отсутствует.")
        end_dt_str = "Неизвестно"
    
    if context.user_data.get('selected_period_type') == 'custom':
        period_text_full = period_display_text 
    else: 
        period_text_full = f"{period_display_text} (с {start_dt_str} по {end_dt_str})"

    # Убедитесь, что функция escape_markdown_v2 определена, если используете ParseMode.MARKDOWN_V2
    # Иначе используйте другой parse_mode или уберите форматирование
    text_message = (
        f"Шаг 3: Подтверждение экспорта\n\n"
        f"Сектор: *{escape_markdown_v2(str(sector_display_name))}*\n"
        f"Период: *{escape_markdown_v2(str(period_text_full))}*\n\n"
        f"Сформировать отчет с этими параметрами?"
    )
    keyboard = [
        [InlineKeyboardButton("✅ Да, сформировать", callback_data="export_confirm_yes")],
        [InlineKeyboardButton("⬅️ Назад (к выбору периода)", callback_data="export_back_to_period_selection")],
        [InlineKeyboardButton("❌ Отменить экспорт", callback_data="export_cancel_dialog")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query: 
        await query.answer()
        try:
            await query.edit_message_text(text=text_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            logger.warning(f"Ошибка при редактировании сообщения в show_export_confirmation: {e}. Отправка нового сообщения.")
            if update.effective_chat:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=text_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    elif update.message: 
        await update.message.reply_text(text=text_message, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        logger.error("show_export_confirmation вызвана без query и без message.")
        if update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка отображения подтверждения.")
        return ConversationHandler.END
        
    return CONFIRM_EXPORT

    
async def process_export_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор НАЧАЛЬНОЙ даты из календаря.
    Если дата выбрана, запрашивает КОНЕЧНУЮ дату.
    Если это навигация по календарю, обновляет календарь.
    """
    query = update.callback_query
    if not query:
        logger.error("process_export_start_date вызвана без callback_query.")
        return ConversationHandler.END
    await query.answer()
    
    # ТВОЙ НАДЕЖНЫЙ БЛОК TRY...EXCEPT - ОСТАВЛЯЕМ!
    try:
        min_cal_date = date(2023, 1, 1) 
        max_cal_date = date.today()
    except NameError:
        logger.error("Имя 'date' не определено. Убедитесь, что 'from datetime import date, datetime' есть в импортах.")
        await query.edit_message_text("Внутренняя ошибка конфигурации даты. Свяжитесь с администратором.")
        return ConversationHandler.END
    
    result, key, step = DetailedTelegramCalendar(
        locale='ru', min_date=min_cal_date, max_date=max_cal_date
    ).process(query.data)

    if not result and key: # Навигация по календарю
        logger.info("process_export_start_date: Навигация по календарю начальной даты.")
        await query.edit_message_text("🗓️ Шаг 2.1: Выберите НАЧАЛЬНУЮ дату для отчета:", reply_markup=key)
        return GET_EXPORT_START_DATE
    elif result: # Дата выбрана
        # МОЙ ФИКС ВРЕМЕНИ
        start_datetime = datetime.combine(result, datetime.min.time())
        context.user_data['export_start_date'] = start_datetime
        logger.info(f"Админ {query.from_user.id} выбрал начальную дату: {start_datetime.strftime('%d.%m.%Y %H:%M')}")
        
        # ТВОЯ ЛОГИКА ЗАПРОСА КОНЕЧНОЙ ДАТЫ - ОСТАВЛЯЕМ!
        end_min_date = result 
        end_max_date = date.today()
        if end_max_date < end_min_date:
             end_max_date = end_min_date

        calendar_end, step_end = DetailedTelegramCalendar(
            locale='ru', min_date=end_min_date, max_date=end_max_date
        ).build()
        await query.edit_message_text(
            f"Начальная дата: {result.strftime('%d.%m.%Y')}\n"
            "🗓️ Шаг 2.2: Теперь выберите КОНЕЧНУЮ дату для отчета:",
            reply_markup=calendar_end
        )
        return GET_EXPORT_END_DATE
    
    # ТВОЯ НАДЕЖНАЯ ОБРАБОТКА ОШИБКИ В КОНЦЕ - ОСТАВЛЯЕМ!
    logger.warning(f"process_export_start_date: дата не выбрана и нет ключа навигации (result={result}, key={key}). Callback: {query.data}")
    await query.edit_message_text(
        "Ошибка при выборе начальной даты. Попробуйте снова или отмените экспорт (/cancel_export)."
    )
    return GET_EXPORT_START_DATE


async def process_export_end_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор КОНЕЧНОЙ даты из календаря.
    Если дата выбрана, переходит к подтверждению экспорта.
    Если это навигация, обновляет календарь.
    """
    query = update.callback_query
    if not query:
        logger.error("process_export_end_date вызвана без callback_query.")
        return ConversationHandler.END
    await query.answer()
    
    # --- ТВОЯ НАДЕЖНАЯ ПРОВЕРКА ---
    # Мы ожидаем, что start_date теперь это datetime объект
    start_datetime_obj = context.user_data.get('export_start_date') 
    if not start_datetime_obj or not isinstance(start_datetime_obj, datetime):
        logger.error(f"Админ {query.from_user.id}: начальная дата (datetime) не найдена или неверного типа.")
        await query.edit_message_text("Ошибка: начальная дата не найдена. Начните выбор периода заново.")
        return await ask_export_period(update, context)

    # Для логики календаря нам нужна "голая" дата, без времени
    start_date_for_calendar = start_datetime_obj.date()

    # --- ТВОЯ НАДЕЖНАЯ ПРОВЕРКА ГРАНИЦ ---
    try:
        end_min_date = start_date_for_calendar
        end_max_date = date.today()
        if end_max_date < end_min_date: 
             end_max_date = end_min_date
    except NameError: 
        logger.critical("Имя 'date' не определено. Убедитесь в правильности импорта 'from datetime import date, datetime'.")
        await query.edit_message_text("Критическая внутренняя ошибка конфигурации даты.")
        return ConversationHandler.END 
         
    result, key, step = DetailedTelegramCalendar(
        locale='ru', min_date=end_min_date, max_date=end_max_date
    ).process(query.data)

    if not result and key: # Навигация по календарю
        await query.edit_message_text(
            f"Начальная дата: {start_date_for_calendar.strftime('%d.%m.%Y')}\n"
            "🗓️ Шаг 2.2: Теперь выберите КОНЕЧНУЮ дату для отчета:",
            reply_markup=key
        )
        return GET_EXPORT_END_DATE
    elif result: # Дата выбрана
        end_date_obj = result
        # --- ТВОЯ ПРОВЕРКА, ЧТО КОНЕЧНАЯ ДАТА НЕ РАНЬШЕ НАЧАЛЬНОЙ ---
        if end_date_obj < start_date_for_calendar:
            logger.info(f"Админ {query.from_user.id}: конечная дата раньше начальной. Запрос повторно.")
            calendar_end_retry, _ = DetailedTelegramCalendar(locale='ru', min_date=start_date_for_calendar, max_date=end_max_date).build()
            await query.edit_message_text(
                f"Начальная дата: {start_date_for_calendar.strftime('%d.%m.%Y')}\n"
                f"⚠️ Конечная дата не может быть раньше начальной. Выберите КОНЕЧНУЮ дату еще раз:",
                reply_markup=calendar_end_retry
            )
            return GET_EXPORT_END_DATE

        # --- МОЙ ФИКС ВРЕМЕНИ (КОНЕЦ ДНЯ) ---
        end_datetime = datetime.combine(end_date_obj, datetime.max.time())
        context.user_data['export_end_date'] = end_datetime
        logger.info(f"Админ {query.from_user.id} выбрал конечную дату: {end_datetime.strftime('%d.%m.%Y %H:%M:%S')}")
        
        # --- ТВОЯ ЛОГИКА СОХРАНЕНИЯ ДАННЫХ ---
        context.user_data['selected_period_type'] = 'custom' 
        context.user_data['export_period_display_text'] = f"с {start_date_for_calendar.strftime('%d.%m.%Y')} по {end_date_obj.strftime('%d.%m.%Y')}"
        
        return await show_export_confirmation(update, context)
    
    # --- ТВОЯ ОБРАБОТКА ОШИБКИ В КОНЦЕ ---
    logger.warning(f"process_export_end_date: дата не выбрана и нет ключа навигации. Callback: {query.data}")
    await query.edit_message_text(
        "Ошибка при выборе конечной даты. Попробуйте снова или отмените экспорт."
    )
    return GET_EXPORT_END_DATE




async def start_export_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начинает диалог экспорта, предлагает выбрать сектор из предопределенного списка."""
    user_id = update.effective_user.id
    logger.info(f"User {user_id} начал диалог экспорта (/admin_export_attendance).")
    
    # Очищаем данные предыдущего диалога экспорта, если они были
    context.user_data.clear() 
    
    # Используем наш предопределенный список секторов
    # Убедитесь, что PREDEFINED_SECTORS определен в вашем файле config.py или глобально
    # Например: PREDEFINED_SECTORS = ["Сектор СС", "Сектор ВИ", "Сектор ОП"]
    sectors_to_display = PREDEFINED_SECTORS 

    keyboard = [
        # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
        [InlineKeyboardButton("Все секторы", callback_data="export_sector_ALL")] 
        # -------------------------
    ]
    
    if sectors_to_display:
        for sector_name in sectors_to_display:
            display_name = str(sector_name).strip() 
            if display_name: 
                # Извлекаем аббревиатуру (например, "СС" из "Сектор СС")
                # Это должно соответствовать частям вашего паттерна (СС|ВИ|ОП)
                parts = display_name.split()
                # Берем последнее слово как ключ (предполагается, что это аббревиатура)
                # и приводим к верхнему регистру для соответствия паттерну, если он ожидает верхний регистр
                callback_sector_value = parts[-1].upper() if len(parts) > 1 else display_name.upper() 

                keyboard.append([InlineKeyboardButton(display_name, callback_data=f"export_sector_{callback_sector_value}")])
    else:
        logger.warning("Предопределенный список секторов (PREDEFINED_SECTORS) пуст! Будет предложен только вариант 'Все секторы'.")

    # Добавляем кнопку "Отмена" в любом случае
    keyboard.append([InlineKeyboardButton("❌ Отменить экспорт", callback_data="export_cancel_dialog")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message_text = "📊 Экспорт посещаемости\n\nШаг 1: Выберите сектор для отчета из списка:"
    
    if update.message: # Если диалог начат командой /admin_export_attendance
        await update.message.reply_text(
            message_text, 
            reply_markup=reply_markup
        )
    elif update.callback_query: # Если это возврат к этому шагу через кнопку "Назад" из другого состояния
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(
                text=message_text,
                reply_markup=reply_markup
            )
        except Exception as e: 
            # Если по какой-то причине редактирование не удалось (например, сообщение слишком старое или изменено),
            # отправляем новое сообщение.
            logger.warning(f"Не удалось отредактировать сообщение при возврате к выбору сектора: {e}. Отправляю новое.")
            await context.bot.send_message(chat_id=user_id, text=message_text, reply_markup=reply_markup)
            
    return SELECT_SECTOR

async def select_sector_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор сектора, сохраняет его и отображаемое имя, 
    затем вызывает ask_export_period для предложения выбрать период.
    """
    query = update.callback_query
    await query.answer()
    selected_sector_data = query.data 
    
    if selected_sector_data == "export_cancel_dialog":
        return await cancel_export_dialog(update, context)

    # Извлекаем ключ сектора (например, "ALL", "СС", "ВИ")
    sector_key = selected_sector_data.replace("export_sector_", "")
    context.user_data['export_selected_sector'] = sector_key
    
    # Определяем и сохраняем отображаемое имя сектора
    # Это имя будет использоваться в функции show_export_confirmation
    sector_display_name = "Все секторы" # По умолчанию для "ALL"
    if sector_key != 'ALL': # Убедитесь, что ключ для "Все секторы" у вас именно 'ALL' (в верхнем регистре)
        # Пытаемся найти полное имя сектора в PREDEFINED_SECTORS
        # Предполагаем, что PREDEFINED_SECTORS - это список строк типа ["Сектор СС", "Сектор ВИ"]
        # и что sector_key (например, "СС") соответствует последнему слову в этих строках.
        found_display_name = False
        if 'PREDEFINED_SECTORS' in globals() and isinstance(PREDEFINED_SECTORS, list):
            for predefined_name in PREDEFINED_SECTORS:
                parts = predefined_name.split()
                # Сравниваем ключ сектора (например, "СС") с последним словом из PREDEFINED_SECTORS
                if len(parts) > 0 and parts[-1].upper() == sector_key.upper():
                    sector_display_name = predefined_name
                    found_display_name = True
                    break
        if not found_display_name:
            # Если не нашли в PREDEFINED_SECTORS, используем ключ как часть имени по умолчанию
            sector_display_name = f"Сектор {sector_key}" 
            logger.warning(f"Для ключа сектора '{sector_key}' не найдено отображаемое имя в PREDEFINED_SECTORS. Используется: '{sector_display_name}'")

    context.user_data['export_sector_display_name'] = sector_display_name
    
    logger.info(f"User {query.from_user.id} выбрал сектор: {sector_key} (Отображаемое имя: {sector_display_name})")
    
    # Вместо формирования клавиатуры здесь, вызываем нашу новую функцию
    return await ask_export_period(update, context)


async def select_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает выбор стандартного периода, рассчитывает даты,
    сохраняет их и затем вызывает show_export_confirmation.
    """
    query = update.callback_query
    await query.answer()
    selected_data = query.data

    if selected_data == "export_cancel_dialog":
        return await cancel_export_dialog(update, context)
    
    if selected_data == "export_back_to_sector_selection":
        # Убедитесь, что start_export_dialog корректно обрабатывает вызов через query
        # и возвращает SELECT_SECTOR
        return await start_export_dialog(update, context) 

    period_key = selected_data.replace("export_period_", "")
    # Сохраняем тип выбранного периода (например, "today", "this_week")
    # Это будет использовано в show_export_confirmation для корректного отображения
    context.user_data['selected_period_type'] = period_key 
    
    logger.info(f"User {query.from_user.id} выбрал стандартный период: {period_key}")

    now = datetime.now()
    start_date_dt = None
    end_date_dt = None
    period_display_text = "" # Это будет отображаемое имя периода, например, "Сегодня"

    if period_key == "today":
        start_date_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = now # До текущего момента
        period_display_text = "Сегодня"
    elif period_key == "yesterday":
        yesterday = now - timedelta(days=1)
        start_date_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_display_text = "Вчера"
    elif period_key == "this_week":
        start_date_dt = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = now # До текущего момента этой недели
        period_display_text = "Эта неделя"
    elif period_key == "last_week":
        last_week_start = now - timedelta(days=now.weekday() + 7)
        start_date_dt = last_week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = (last_week_start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)
        period_display_text = "Прошлая неделя"
    elif period_key == "this_month":
        start_date_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = now # До текущего момента этого месяца
        period_display_text = "Этот месяц"
    elif period_key == "last_month":
        first_day_of_current_month = now.replace(day=1)
        last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        start_date_dt = first_day_of_last_month.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date_dt = last_day_of_last_month.replace(hour=23, minute=59, second=59, microsecond=999999)
        period_display_text = "Прошлый месяц"
    # Добавьте сюда другие стандартные периоды, если они есть

    if start_date_dt and end_date_dt:
        context.user_data['export_start_date'] = start_date_dt # datetime.datetime объект
        context.user_data['export_end_date'] = end_date_dt   # datetime.datetime объект
        context.user_data['export_period_display_text'] = period_display_text # Например, "Сегодня", "Эта неделя"
        
        logger.info(f"Рассчитан стандартный период: {period_display_text} с {start_date_dt.strftime('%Y-%m-%d %H:%M:%S')} по {end_date_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Вместо формирования текста и клавиатуры подтверждения здесь,
        # вызываем нашу новую функцию show_export_confirmation.
        # Она сама возьмет 'export_sector_display_name', 'export_start_date', 
        # 'export_end_date', 'export_period_display_text' и 'selected_period_type' из context.user_data.
        return await show_export_confirmation(update, context)
    else:
        logger.error(f"Не удалось рассчитать даты для ключа стандартного периода: {period_key}. start_date_dt: {start_date_dt}, end_date_dt: {end_date_dt}")
        await query.edit_message_text("Произошла ошибка при выборе периода. Попробуйте снова.")
        # Возвращаемся к шагу выбора периода, используя нашу новую функцию
        return await ask_export_period(update, context)


# Вспомогательная функция для форматирования секунд в ЧЧ:ММ:СС
def format_seconds_to_hhmmss(seconds_val):
    # Если на входе некорректные данные (например, сессия еще не закрыта),
    # возвращаем "Активна", что гораздо понятнее, чем "00:00:00".
    if pd.isna(seconds_val) or not isinstance(seconds_val, (int, float)) or seconds_val < 0:
        return "Активна" 
    
    seconds_val = int(seconds_val)
    hours = seconds_val // 3600
    minutes = (seconds_val % 3600) // 60
    secs = seconds_val % 60
    
    # f-строка с :02d - это то же самое, что :02
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


async def generate_custom_excel_report(report_data: list, report_info: dict, selected_sector_key: str) -> bytes:
    """
    Генерирует Excel-отчет, САМОСТОЯТЕЛЬНО ВЫЧИСЛЯЯ длительность сессий
    и ДОБАВЛЯЯ ОТСТУПЫ между сотрудниками.
    """
    if not report_data:
        # ... (эта часть остается без изменений)
        logger.info("Нет данных для генерации Excel отчета.")
        df_empty = pd.DataFrame([{"Сообщение": "Нет данных для отображения в выбранных параметрах"}])
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_empty.to_excel(writer, sheet_name="Отчет", index=False)
        return output.getvalue()

    df_all_data = pd.DataFrame(report_data)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        def prepare_and_write_sheet(df_sheet_data, sheet_name_param):
            if df_sheet_data.empty:
                return

            df_details = df_sheet_data.copy()

            start_times = pd.to_datetime(df_details['session_start_time'], errors='coerce')
            end_times = pd.to_datetime(df_details['session_end_time'], errors='coerce')
            duration = end_times - start_times
            df_details['Длительность сессии'] = duration.dt.total_seconds().apply(format_seconds_to_hhmmss)
            
            df_details.rename(columns={
                'application_full_name': 'ФИО',
                'username': 'Telegram Username',
                'application_department': 'Сектор',
                'session_start_time': 'Начало сессии',
                'session_end_time': 'Конец сессии'
            }, inplace=True)
            
            final_columns = ['ФИО', 'Telegram Username', 'Сектор', 'Начало сессии', 'Конец сессии', 'Длительность сессии']
            existing_columns = [col for col in final_columns if col in df_details.columns]
            df_sheet_final = df_details[existing_columns]

            # --- НАЧАЛО НОВОЙ ЛОГИКИ ОТСТУПОВ ---
            
            # Убедимся, что данные отсортированы по ФИО, это ключ к успеху
            df_sorted = df_sheet_final.sort_values(by='ФИО').reset_index(drop=True)
            
            new_rows = []
            last_name = None
            
            # Создаем шаблон для пустой строки, чтобы сохранить структуру колонок
            blank_row = pd.Series([''] * len(df_sorted.columns), index=df_sorted.columns)

            for index, row in df_sorted.iterrows():
                current_name = row['ФИО']
                # Если имя сменилось (и это не первая строка), добавляем пустую строку
                if last_name is not None and current_name != last_name:
                    new_rows.append(blank_row)
                
                new_rows.append(row) # Добавляем текущую строку с данными
                last_name = current_name
            
            # Создаем новый DataFrame из списка, который теперь содержит пустые строки
            df_with_spacing = pd.DataFrame(new_rows)
            
            # --- КОНЕЦ НОВОЙ ЛОГИКИ ОТСТУПОВ ---

            # Записываем в Excel новый DataFrame с отступами
            df_with_spacing.to_excel(writer, sheet_name=sheet_name_param, index=False)
            
            # ... (остальная часть функции по форматированию листа остается без изменений)
            worksheet = writer.book[sheet_name_param]   
            for column_cells in worksheet.columns:
                try:
                    max_length = 0
                    column_letter = column_cells[0].column_letter
                    for cell in column_cells:
                        if cell.value is not None:
                            cell_length = len(str(cell.value))
                            if cell_length > max_length:
                                max_length = cell_length
                    adjusted_width = (max_length + 2) if max_length > 0 else 10
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                except Exception as e_width:
                    logger.debug(f"Ошибка автоширины для столбца на листе '{sheet_name_param}': {e_width}")
            logger.info(f"Лист '{sheet_name_param}' успешно добавлен в Excel.")

        # ... (остальная часть функции по разделению на листы остается без изменений)
        if selected_sector_key.upper() == 'ALL':
            if 'application_department' not in df_all_data.columns:
                logger.error("Столбец 'application_department' отсутствует в данных, не могу разделить по секторам.")
                prepare_and_write_sheet(df_all_data, "Все_данные_ошибка_группировки")
            else:
                df_all_data['normalized_department_for_sheet'] = df_all_data['application_department'].astype(str).str.upper().fillna('Без_сектора')
                unique_departments_for_sheets = df_all_data['normalized_department_for_sheet'].unique()
                for dept_sheet_name_base in unique_departments_for_sheets:
                    safe_sheet_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in dept_sheet_name_base)[:31]
                    df_dept_data = df_all_data[df_all_data['normalized_department_for_sheet'] == dept_sheet_name_base]
                    if not df_dept_data.empty:
                        prepare_and_write_sheet(df_dept_data.copy(), safe_sheet_name)
        else:
            sheet_name_base = report_info.get('sector_display_name', 'Детализация').replace(' ', '_')
            safe_sheet_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in sheet_name_base)[:31]
            prepare_and_write_sheet(df_all_data, safe_sheet_name)

    logger.info("Excel-файл сгенерирован в памяти.")
    return output.getvalue()





async def confirm_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обрабатывает подтверждение экспорта, запускает генерацию отчета 
    или возвращает к предыдущему шагу/отменяет диалог.
    """
    query = update.callback_query
    await query.answer()
    user_choice = query.data
    admin_chat_id = query.from_user.id

    if user_choice == "export_cancel_dialog":
        return await cancel_export_dialog(update, context)

    if user_choice == "export_back_to_period_selection":
        # Вместо формирования клавиатуры здесь, вызываем нашу новую функцию
        logger.info(f"User {admin_chat_id} нажал 'Назад (к выбору периода)' из подтверждения. Вызываем ask_export_period.")
        return await ask_export_period(update, context) # [[1]] (Это просто пример цитаты, не обращайте внимания)

    if user_choice == "export_confirm_yes":
        logger.debug(f"В confirm_export_callback, перед извлечением данных: context.user_data={context.user_data}")
        
        selected_sector_key = context.user_data.get('export_selected_sector', 'ALL')
        start_date_dt = context.user_data.get('export_start_date')
        end_date_dt = context.user_data.get('export_end_date')
        
        period_display_text = context.user_data.get('export_period_display_text', 'Неизвестный период')
        sector_display_name = context.user_data.get('export_sector_display_name', 'Все секторы') 

        if not start_date_dt or not end_date_dt:
            logger.error(f"Не найдены start_date ({start_date_dt}) или end_date ({end_date_dt}) в context.user_data для экспорта. User_data: {context.user_data}")
            await query.edit_message_text("Ошибка: не удалось определить период для отчета. Пожалуйста, начните заново.")
            context.user_data.clear()
            return ConversationHandler.END

        logger.info(f"User {admin_chat_id} подтвердил экспорт. Сектор: {selected_sector_key} ({sector_display_name}), Период: {period_display_text} (с {start_date_dt.strftime('%Y-%m-%d %H:%M')} по {end_date_dt.strftime('%Y-%m-%d %H:%M')})")
        
        await query.edit_message_text(text="Отлично! Начинаю формирование отчета... Это может занять некоторое время.")
        
        try:
            report_data_from_db = db.get_attendance_data_for_period(
                start_date=start_date_dt, 
                end_date=end_date_dt, 
                sector_key=selected_sector_key
            )
            logger.info(f"Получено {len(report_data_from_db)} записей из БД для отчета.")

            if not report_data_from_db:
                await context.bot.send_message(
                    chat_id=admin_chat_id,
                    text=f"Нет данных о посещаемости для сектора '{sector_display_name}' за период '{period_display_text}'. Отчет не будет сформирован."
                )
                context.user_data.clear() # Очищаем данные, так как диалог завершен
                return ConversationHandler.END

            report_info_for_excel = {
                "sector_display_name": sector_display_name,
                "period_display_text": period_display_text,
                "start_date_str": start_date_dt.strftime('%d.%m.%Y'),
                "end_date_str": end_date_dt.strftime('%d.%m.%Y')
            }
            
            excel_bytes = await generate_custom_excel_report(
                report_data_from_db, 
                report_info_for_excel,
                selected_sector_key 
            )
            
            if excel_bytes:
                filename_sector_part = sector_display_name.replace(' ', '_').replace('Сектор_', '')
                if selected_sector_key == 'ALL':
                    filename_sector_part = "Все_секторы"
                
                # Форматируем даты для имени файла, убедившись, что это объекты date/datetime
                start_date_filename = start_date_dt.strftime('%Y%m%d') if hasattr(start_date_dt, 'strftime') else "nodate"
                end_date_filename = end_date_dt.strftime('%Y%m%d') if hasattr(end_date_dt, 'strftime') else "nodate"

                filename = f"Отчет_{filename_sector_part}_{start_date_filename}-{end_date_filename}.xlsx"
                
                caption_text = (
                    f"Отчет о посещаемости.\n"
                    f"Сектор: {sector_display_name}\n"
                    f"Период: {period_display_text}"
                )
                # Добавляем даты в caption, если они есть и корректно форматируются
                if hasattr(start_date_dt, 'strftime') and hasattr(end_date_dt, 'strftime'):
                    caption_text += f" (с {start_date_dt.strftime('%d.%m.%Y %H:%M')} по {end_date_dt.strftime('%d.%m.%Y %H:%M')})"

                await context.bot.send_document(
                    chat_id=admin_chat_id, 
                    document=excel_bytes, 
                    filename=filename,
                    caption=caption_text
                )
                logger.info(f"Отчет '{filename}' успешно отправлен администратору {admin_chat_id}.")
            else:
                logger.error("Функция generate_custom_excel_report не вернула данные файла.")
                await context.bot.send_message(chat_id=admin_chat_id, text="Не удалось сгенерировать Excel-файл. Проверьте логи для деталей.")
        except Exception as e:
            logger.error(f"Критическая ошибка при формировании или отправке отчета: {e}", exc_info=True)
            await context.bot.send_message(chat_id=admin_chat_id, text="Произошла серьезная ошибка при формировании отчета. Обратитесь к администратору бота.")
        finally:
            context.user_data.clear()
            logger.debug("context.user_data очищен после попытки экспорта.")
            
        return ConversationHandler.END
    
    # Если user_choice не соответствует ни одному из ожидаемых, можно добавить обработку
    logger.warning(f"Неизвестный user_choice в confirm_export_callback: {user_choice}")
    return ConversationHandler.END # Или другое подходящее состояние



async def cancel_export_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отменяет диалог экспорта."""
    user_id = update.effective_user.id
    logger.info(f"User {user_id} отменил диалог экспорта.")
    
    query = update.callback_query
    message_to_edit_or_send = "Экспорт отменен."

    if query:
        await query.answer()
        try:
            await query.edit_message_text(text=message_to_edit_or_send)
        except Exception as e: # Если сообщение не может быть отредактировано (например, слишком старое)
            logger.debug(f"Не удалось отредактировать сообщение при отмене диалога: {e}")
            await context.bot.send_message(chat_id=user_id, text=message_to_edit_or_send)
    elif update.message: # Если отмена была через команду /cancel_export
        await update.message.reply_text(message_to_edit_or_send)
            
    context.user_data.clear()
    return ConversationHandler.END

async def ask_export_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отправляет календарь для выбора НАЧАЛЬНОЙ даты отчета."""
    query = update.callback_query
    if not query:
        logger.error("ask_export_start_date вызвана без callback_query. Это неожиданно.")
        if update.effective_chat:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка при запросе календаря. Попробуйте снова.")
        return ConversationHandler.END 
    
    await query.answer() 

    # Используем импортированный 'date' напрямую
    try:
        min_selectable_date = date(2023, 1, 1)  
        max_selectable_date = date.today()      
    except Exception as e: # На всякий случай, если другая ошибка при создании дат
        logger.error(f"Неожиданная ошибка при создании дат в ask_export_start_date: {e}", exc_info=True)
        await query.edit_message_text("Произошла внутренняя ошибка при формировании календаря. Пожалуйста, попробуйте позже.")
        return GET_EXPORT_START_DATE # Или ConversationHandler.END

    calendar, step = DetailedTelegramCalendar(
        locale='ru', min_date=min_selectable_date, max_date=max_selectable_date
    ).build()
    
    message_text = "🗓️ Шаг 2.1: Выберите НАЧАЛЬНУЮ дату для отчета:"
    
    await query.edit_message_text(
        text=message_text,
        reply_markup=calendar
    )
        
    return GET_EXPORT_START_DATE


# async def get_custom_date_start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает начальную дату для кастомного периода."""
    # TODO: Реализовать запрос начальной даты
    logger.info(f"User {update.effective_user.id} выбрал кастомный период, запрашиваем начальную дату.")
    await update.callback_query.edit_message_text(
        text="Введите начальную дату для отчета в формате ДД.ММ.ГГГГ:"
    )
    return GET_CUSTOM_DATE_START # Состояние ожидания текстового ввода начальной даты

# async def process_custom_date_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенную начальную дату, запрашивает конечную."""
    user_input = update.message.text
    user_id = update.effective_user.id
    logger.info(f"User {user_id} ввел начальную дату: {user_input}")
    # TODO: Валидация и сохранение даты context.user_data['export_start_date'] = parsed_date
    # Пока просто сохраняем как есть
    context.user_data['export_start_date_str'] = user_input 
    
    await update.message.reply_text("Отлично! Теперь введите конечную дату для отчета в формате ДД.ММ.ГГГГ:")
    return GET_CUSTOM_DATE_END # Состояние ожидания текстового ввода конечной даты



async def process_custom_date_end(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенную конечную дату, переходит к подтверждению."""
    user_input = update.message.text
    user_id = update.effective_user.id
    logger.info(f"User {user_id} ввел конечную дату: {user_input}")
    # TODO: Валидация и сохранение даты context.user_data['export_end_date'] = parsed_date
    context.user_data['export_end_date_str'] = user_input

    # После получения обеих дат, нужно сформировать текст подтверждения и кнопки
    # Это похоже на логику в select_period_callback, но с кастомными датами
    sector_display = context.user_data.get('export_selected_sector', 'Не выбран')
    start_date_str = context.user_data.get('export_start_date_str', 'Не указана')
    end_date_str = context.user_data.get('export_end_date_str', 'Не указана')

    confirmation_text = (
        f"Вы выбрали для экспорта:\n"
        f"🔹 Сектор: **{sector_display.upper() if sector_display != 'all' else 'Все секторы'}**\n"
        f"🔹 Период: с **{start_date_str}** по **{end_date_str}**\n\n"
        f"Сформировать отчет с этими параметрами?"
    )
    keyboard = [
        [InlineKeyboardButton("✅ Да, сформировать", callback_data="export_confirm_yes")],
        [InlineKeyboardButton("✏️ Назад (к выбору периода)", callback_data="export_back_to_period_selection")], # Или к вводу дат?
        [InlineKeyboardButton("❌ Отмена", callback_data="export_cancel_dialog")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(text=confirmation_text, reply_markup=reply_markup, parse_mode='Markdown')
    return CONFIRM_EXPORT



@admin_required
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    await update.message.reply_text(escape_markdown_v2("♻️ Перезапуск бота..."), parse_mode=ParseMode.MARKDOWN_V2)
    logger.info(f"Бот перезапускается администратором {user_id} ({update.effective_user.username})")
    try:
        await context.application.updater.stop()
        os.execv(sys.executable, ['python'] + sys.argv)
    except Exception as e:
        logger.error(f"Ошибка при попытке перезапуска: {e}")
        await update.message.reply_text(escape_markdown_v2(f"⚠️ Не удалось автоматически перезапустить бота. Ошибка: {e}"), parse_mode=ParseMode.MARKDOWN_V2)

@admin_required
async def admin_authorize_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_user = update.effective_user
    if not context.args:
        text_to_send = "Пожалуйста, укажите Telegram ID пользователя\\.\nИспользование: /admin\\_authorize `USER_ID`"
        await update.message.reply_text(text_to_send, parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        user_id_to_authorize = int(context.args[0])
        success, message_from_db = db.authorize_user(user_id_to_authorize, admin_user.id)
        await update.message.reply_text(escape_markdown_v2(message_from_db), parse_mode=ParseMode.MARKDOWN_V2)
        if success and "успешно авторизован" in message_from_db:
            target_user_info = db.get_user(user_id_to_authorize)
            if target_user_info:
                try:
                    await context.bot.send_message(
                        chat_id=user_id_to_authorize,
                        text="🎉 Поздравляем! Администратор подтвердил вашу авторизацию. Теперь вы можете использовать все функции бота."
                    )
                    logger.info(f"Уведомление об авторизации отправлено пользователю {user_id_to_authorize}")
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление об авторизации пользователю {user_id_to_authorize}: {e}")
            else:
                logger.warning(f"Не удалось получить данные пользователя {user_id_to_authorize} после успешной авторизации для отправки уведомления.")
    except ValueError:
        text_to_send = "Telegram ID должен быть числом\\.\nИспользование: /admin\\_authorize `USER_ID`"
        await update.message.reply_text(text_to_send, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Ошибка в admin_authorize_command: {e}", exc_info=True)
        await update.message.reply_text(escape_markdown_v2("Произошла непредвиденная ошибка при авторизации."), parse_mode=ParseMode.MARKDOWN_V2)


async def _send_pending_list_message(
    context: ContextTypes.DEFAULT_TYPE, 
    chat_id: int, 
    page: int = 1, # Номер текущей страницы
    focused_user_id: int = None, 
    edit_message_id: int = None
):
    pending_users_full_list = db.list_pending_users()
    total_items = len(pending_users_full_list)

    if total_items == 0:
        message_text = escape_markdown_v2("Нет пользователей, ожидающих авторизации.")
        reply_markup_to_send = None
        # ... (код для отправки/редактирования сообщения о пустом списке, как было)
        if edit_message_id:
            try:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=edit_message_id, text=message_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=reply_markup_to_send)
            except Exception as e:
                logger.warning(f"Не удалось отредактировать сообщение для пустого списка: {e}")
                await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, parse_mode=ParseMode.MARKDOWN_V2)
        return

    total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages)) # Убедимся, что страница в допустимых пределах

    # Определяем срез пользователей для текущей страницы
    start_index = (page - 1) * ITEMS_PER_PAGE
    end_index = start_index + ITEMS_PER_PAGE
    current_page_items = pending_users_full_list[start_index:end_index]

    keyboard_buttons_list = []
    
    header_text_parts = [f"*Пользователи, ожидающие авторизации \\(Стр\\. {page}/{total_pages}, Всего: {total_items}\\):*"]
    if focused_user_id:
        focused_user_info = next((p for p in pending_users_full_list if p['telegram_id'] == focused_user_id), None)
        if focused_user_info:
            focused_name = focused_user_info['application_full_name'] or f"ID {focused_user_id}"
            # Проверяем, есть ли focused_user_id на текущей странице
            if any(item['telegram_id'] == focused_user_id for item in current_page_items):
                 header_text_parts.append(f"Заявка от *{escape_markdown_v2(focused_name)}* отмечена символом 🎯 на этой странице\\.")
            else:
                 header_text_parts.append(f"Заявка от *{escape_markdown_v2(focused_name)}* находится на другой странице\\.")
        else: # Если focused_user_id не найден в общем списке (маловероятно, но возможно)
            header_text_parts.append(f"Заявка от пользователя ID `{focused_user_id}` (не найдена) была запрошена для подсветки\\.")

    header_text_parts.append("Выберите пользователя для просмотра деталей заявки\\.")
    final_header_text = "\n".join(header_text_parts)

    # Формируем кнопки для пользователей на текущей странице
    for i, user_row in enumerate(current_page_items):
        user_id = user_row['telegram_id']
        # Глобальный номер элемента в списке (для отображения)
        global_item_number = start_index + i + 1 
        app_full_name_raw = user_row['application_full_name'] if user_row['application_full_name'] else "ФИО не указано"
        app_department_raw = user_row['application_department'] if user_row['application_department'] else "Сектор не указан"
        
        prefix = "🎯 " if focused_user_id and user_id == focused_user_id else ""
        # Используем global_item_number для нумерации
        button_text_unescaped = f"{prefix}{global_item_number}. {app_full_name_raw}, {app_department_raw}"
        
        MAX_BUTTON_TEXT_BYTES = 60 
        temp_button_text = button_text_unescaped
        while len(temp_button_text.encode('utf-8')) > MAX_BUTTON_TEXT_BYTES:
            temp_button_text = temp_button_text[:-4] + "..." 
            if len(temp_button_text) < 5: 
                temp_button_text = "Инфо..." 
                break
        final_button_text = temp_button_text
        
        keyboard_buttons_list.append([
            InlineKeyboardButton(final_button_text, callback_data=f"view_user_app:{user_id}")
        ])

    # Формируем кнопки пагинации
    pagination_row = []
    focused_id_for_callback = focused_user_id if focused_user_id else 0 # 0 если нет фокуса
    
    if page > 1:
        pagination_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"paginate_list:{page-1}:{focused_id_for_callback}"))
    
    # Кнопка с номером страницы (некликабельная или можно сделать ее обновляющей текущую страницу)
    pagination_row.append(InlineKeyboardButton(f"Стр. {page}/{total_pages}", callback_data="_")) # "_" означает "ничего не делать"

    if page < total_pages:
        pagination_row.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"paginate_list:{page+1}:{focused_id_for_callback}"))
    
    if pagination_row: # Добавляем ряд с кнопками пагинации, если они есть
        keyboard_buttons_list.append(pagination_row)
        
    reply_markup_to_send = InlineKeyboardMarkup(keyboard_buttons_list) if keyboard_buttons_list else None
    
    try:
        if edit_message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id, 
                message_id=edit_message_id, 
                text=final_header_text, 
                reply_markup=reply_markup_to_send, 
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id, 
                text=final_header_text, 
                reply_markup=reply_markup_to_send, 
                parse_mode=ParseMode.MARKDOWN_V2
            )
    except Exception as e:
        logger.error(f"Ошибка при отправке/редактировании списка ожидающих с пагинацией: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text=escape_markdown_v2("Не удалось отобразить список заявок."), parse_mode=ParseMode.MARKDOWN_V2)


@admin_required
async def admin_pending_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_pending_list_message(
        context, 
        update.effective_chat.id, 
        page=1, # Всегда начинаем с первой страницы
        focused_user_id=None, 
        edit_message_id=None
    )


async def admin_action_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    logger.info(f"АДМИН КНОПКА: Получен callback_query с data: '{query.data}' от пользователя {query.from_user.id}")
    await query.answer() 
    
    admin_user = query.from_user
    
    if admin_user.id not in ADMIN_TELEGRAM_IDS:
        logger.warning(f"Не админ {admin_user.id} попытался использовать админскую callback-кнопку: {query.data}")
        try:
            await query.edit_message_text(
                text=query.message.text + "\n\n" + escape_markdown_v2("⚠️ Ошибка: Действие не выполнено (недостаточно прав)."),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=None
            )
        except Exception: pass
        return

    action = ""
    target_user_id = 0 # Для ID пользователя или номера страницы
    focused_user_id_from_callback = None # Для ID пользователя при пагинации

    try:
        parts = query.data.split(':', 2)
        action = parts[0]

        if action == "paginate_list":
            if len(parts) == 3:
                target_page_str = parts[1]
                focused_user_id_str = parts[2]
                target_user_id = int(target_page_str) 
                focused_user_id_from_callback = int(focused_user_id_str) if focused_user_id_str != "0" else None
            else: 
                raise ValueError("Неверный формат callback_data для paginate_list")
        elif action in ["view_user_app", "card_auth_app", "card_reject_app", "focus_in_list"]:
            if len(parts) >= 2:
                target_user_id_str = parts[1]
                target_user_id = int(target_user_id_str)
            else:
                raise ValueError(f"Неверный формат callback_data для {action}")
        else:
            raise ValueError("Неизвестный action")

    except ValueError as e:
        logger.error(f"Ошибка парсинга callback_data: {e} для '{query.data}'", exc_info=True)
        try:
            await query.edit_message_text(
                text=query.message.text + "\n\n" + escape_markdown_v2("⚠️ Ошибка: Некорректные данные кнопки."),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=None
            )
        except Exception: pass
        return

    if action == "paginate_list":
        target_page = target_user_id 
        logger.info(f"АДМИН ДЕЙСТВИЕ: Пагинация списка. Админ {admin_user.id} запросил стр. {target_page}, фокус на {focused_user_id_from_callback}.")
        await _send_pending_list_message(
            context,
            query.message.chat_id,
            page=target_page,
            focused_user_id=focused_user_id_from_callback,
            edit_message_id=query.message.message_id
        )
    
    elif action == "focus_in_list":
        focused_user_id_to_find = target_user_id 
        logger.info(f"АДМИН ДЕЙСТВИЕ: Администратор {admin_user.id} запросил список с фокусом на пользователе {focused_user_id_to_find}.")
        
        pending_users_temp = db.list_pending_users()
        target_page_for_focus = 1 
        if pending_users_temp:
            try:
                user_index = next(i for i, user_val in enumerate(pending_users_temp) if user_val['telegram_id'] == focused_user_id_to_find)
                target_page_for_focus = math.floor(user_index / ITEMS_PER_PAGE) + 1
            except StopIteration:
                logger.warning(f"При focus_in_list пользователь {focused_user_id_to_find} не найден в списке ожидающих. Показываем первую страницу.")
        
        await _send_pending_list_message(
            context, 
            query.message.chat_id,
            page=target_page_for_focus, 
            focused_user_id=focused_user_id_to_find, 
            edit_message_id=query.message.message_id 
        )

    elif action == "view_user_app":
        logger.info(f"АДМИН ДЕЙСТВИЕ: Администратор {admin_user.id} просматривает заявку пользователя {target_user_id}.")
        user_data = db.get_user(target_user_id)

        if not user_data:
            error_msg = escape_markdown_v2(f"Не удалось найти данные для пользователя ID {target_user_id}.")
            try:
                await query.edit_message_text(text=error_msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            except Exception:
                await context.bot.send_message(chat_id=admin_user.id, text=error_msg, parse_mode=ParseMode.MARKDOWN_V2)
            return

        username_str = f"\\(@{escape_markdown_v2(user_data['username'])}\\)" if user_data['username'] else escape_markdown_v2("(нет username)")
        tg_first_name = escape_markdown_v2(user_data['first_name'] or "")
        tg_last_name = escape_markdown_v2(user_data['last_name'] or "")
        app_full_name = escape_markdown_v2(user_data['application_full_name'] or "не указано")
        app_department = escape_markdown_v2(user_data['application_department'] or "не указан")
        
        reg_date_val = user_data['registration_date']
        reg_date = None
        if isinstance(reg_date_val, str):
            try:
                reg_date_val_no_ms = reg_date_val.split('.')[0]
                reg_date = datetime.strptime(reg_date_val_no_ms, '%Y-%m-%d %H:%M:%S')
            except ValueError: pass
        elif isinstance(reg_date_val, datetime): reg_date = reg_date_val
        reg_date_str = escape_markdown_v2(reg_date.strftime('%Y-%m-%d %H:%M')) if reg_date else escape_markdown_v2("N/A")

        status_map = {
            'pending': '⏳ Ожидает решения',
            'approved': '✅ Авторизован',
            'rejected': '🚫 Отклонен',
            'none': '📝 Заявка не подана (или статус неизвестен)'
        }
        current_status = escape_markdown_v2(status_map.get(user_data['application_status'], user_data['application_status']))

        card_text = (
            f"*Карточка заявки пользователя ID `{target_user_id}`*\n"
            f"Имя в Telegram: {tg_first_name} {tg_last_name} {username_str}\n"
            f"ФИО из заявки: {app_full_name}\n"
            f"Сектор из заявки: {app_department}\n"
            f"Дата регистрации в боте: {reg_date_str}\n"
            f"Текущий статус: {current_status}\n"
        )

        action_buttons = []
        if user_data['application_status'] == 'pending':
            action_buttons = [
                InlineKeyboardButton("✅ Авторизовать", callback_data=f"card_auth_app:{target_user_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"card_reject_app:{target_user_id}")
            ]
        
        keyboard_layout = []
        if action_buttons:
            keyboard_layout.append(action_buttons)
        
        card_reply_markup = InlineKeyboardMarkup(keyboard_layout) if keyboard_layout else None
        
        try:
            await query.edit_message_text(text=card_text, reply_markup=card_reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            logger.error(f"Ошибка при отображении карточки пользователя {target_user_id}: {e}", exc_info=True)
            await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2("Не удалось отобразить карточку пользователя."), parse_mode=ParseMode.MARKDOWN_V2)

    elif action == "card_auth_app":
        logger.info(f"АДМИН ДЕЙСТВИЕ: Администратор {admin_user.id} нажал 'Авторизовать' с карточки для {target_user_id}.")
        success, message_from_db = db.authorize_user(target_user_id, admin_user.id)
        
        feedback_to_admin = message_from_db
        if success and "успешно авторизован" in message_from_db:
            feedback_to_admin = f"✅ Пользователь ID {target_user_id} авторизован."
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text="🎉 Поздравляем! Администратор подтвердил вашу авторизацию. Теперь вы можете использовать все функции бота."
                )
                logger.info(f"Уведомление об авторизации отправлено пользователю {target_user_id}")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление об авторизации пользователю {target_user_id}: {e}", exc_info=True)
        else:
            logger.warning(f"db.authorize_user для {target_user_id} (с карточки) не вернуло ожидаемый успешный результат. Success: {success}, Message: {message_from_db}")
        
        try: 
            user_data_for_card = db.get_user(target_user_id)
            if user_data_for_card:
                username_str_card = f"\\(@{escape_markdown_v2(user_data_for_card['username'])}\\)" if user_data_for_card['username'] else escape_markdown_v2("(нет username)")
                tg_first_name_card = escape_markdown_v2(user_data_for_card['first_name'] or "")
                tg_last_name_card = escape_markdown_v2(user_data_for_card['last_name'] or "")
                app_full_name_card = escape_markdown_v2(user_data_for_card['application_full_name'] or "не указано")
                app_department_card = escape_markdown_v2(user_data_for_card['application_department'] or "не указан") 
                
                reg_date_val_card = user_data_for_card['registration_date']
                reg_date_card = None
                if isinstance(reg_date_val_card, str):
                    try: reg_date_card = datetime.strptime(reg_date_val_card.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    except ValueError: pass
                elif isinstance(reg_date_val_card, datetime): reg_date_card = reg_date_val_card
                reg_date_str_card = escape_markdown_v2(reg_date_card.strftime('%Y-%m-%d %H:%M')) if reg_date_card else escape_markdown_v2("N/A")

                final_status_text = escape_markdown_v2(feedback_to_admin) 

                reconstructed_card_text = (
                    f"*Карточка заявки пользователя ID `{target_user_id}`*\n"
                    f"Имя в Telegram: {tg_first_name_card} {tg_last_name_card} {username_str_card}\n"
                    f"ФИО из заявки: {app_full_name_card}\n"
                    f"Сектор из заявки: {app_department_card}\n" 
                    f"Дата регистрации в боте: {reg_date_str_card}\n"
                    f"*Результат обработки:* {final_status_text}\n"
                )
                await query.edit_message_text(text=reconstructed_card_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            else: 
                logger.warning(f"Не удалось получить данные пользователя {target_user_id} для обновления карточки после авторизации.")
                await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2(f"Результат по заявке ID {target_user_id}: {feedback_to_admin}"), parse_mode=ParseMode.MARKDOWN_V2)

        except Exception as e:
             logger.warning(f"Не удалось отредактировать карточку после авторизации {target_user_id}: {e}", exc_info=True)
             await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2(f"Результат по заявке ID {target_user_id}: {feedback_to_admin}"), parse_mode=ParseMode.MARKDOWN_V2)

    elif action == "card_reject_app":
        logger.info(f"АДМИН ДЕЙСТВИЕ: Администратор {admin_user.id} нажал 'Отклонить' с карточки для {target_user_id}.")
        success, message_from_db = db.reject_application(target_user_id, admin_user.id, reason=None) 
        
        feedback_to_admin = message_from_db 
        if success and "успешно отклонена" in message_from_db:
            feedback_to_admin = f"🚫 Заявка пользователя ID {target_user_id} отклонена."
            try:
                user_notification = "К сожалению, ваша заявка на доступ к боту была отклонена администратором."
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=escape_markdown_v2(user_notification),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                logger.info(f"Уведомление об отклонении заявки отправлено пользователю {target_user_id}.")
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление об отклонении заявки пользователю {target_user_id}: {e}", exc_info=True)
        else:
            logger.warning(f"db.reject_application для {target_user_id} (с карточки) не вернуло ожидаемый успешный результат. Success: {success}, Message: {message_from_db}")

        try: 
            user_data_for_card = db.get_user(target_user_id)
            if user_data_for_card:
                username_str_card = f"\\(@{escape_markdown_v2(user_data_for_card['username'])}\\)" if user_data_for_card['username'] else escape_markdown_v2("(нет username)")
                tg_first_name_card = escape_markdown_v2(user_data_for_card['first_name'] or "")
                tg_last_name_card = escape_markdown_v2(user_data_for_card['last_name'] or "")
                app_full_name_card = escape_markdown_v2(user_data_for_card['application_full_name'] or "не указано")
                app_department_card = escape_markdown_v2(user_data_for_card['application_department'] or "не указан") 
                
                reg_date_val_card = user_data_for_card['registration_date']
                reg_date_card = None
                if isinstance(reg_date_val_card, str):
                    try: reg_date_card = datetime.strptime(reg_date_val_card.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    except ValueError: pass
                elif isinstance(reg_date_val_card, datetime): reg_date_card = reg_date_val_card
                reg_date_str_card = escape_markdown_v2(reg_date_card.strftime('%Y-%m-%d %H:%M')) if reg_date_card else escape_markdown_v2("N/A")

                final_status_text = escape_markdown_v2(feedback_to_admin)

                reconstructed_card_text = (
                    f"*Карточка заявки пользователя ID `{target_user_id}`*\n"
                    f"Имя в Telegram: {tg_first_name_card} {tg_last_name_card} {username_str_card}\n"
                    f"ФИО из заявки: {app_full_name_card}\n"
                    f"Сектор из заявки: {app_department_card}\n"
                    f"Дата регистрации в боте: {reg_date_str_card}\n"
                    f"*Результат обработки:* {final_status_text}\n"
                )
                await query.edit_message_text(text=reconstructed_card_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=None)
            else: 
                logger.warning(f"Не удалось получить данные пользователя {target_user_id} для обновления карточки.")
                await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2(f"Результат по заявке ID {target_user_id}: {feedback_to_admin}"), parse_mode=ParseMode.MARKDOWN_V2)

        except Exception as e:
            logger.warning(f"Не удалось отредактировать карточку после отклонения {target_user_id}: {e}", exc_info=True)
            await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2(f"Результат по заявке ID {target_user_id}: {feedback_to_admin}"), parse_mode=ParseMode.MARKDOWN_V2)
    
    else:
        logger.warning(f"Неизвестное действие в admin_action_callback_handler: {query.data}")
        try:
            # Пытаемся отредактировать исходное сообщение, если оно было от кнопки
            if query.message:
                 await query.edit_message_text(
                    text=query.message.text + "\n\n" + escape_markdown_v2("⚠️ Неизвестное действие."), 
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=None
                )
            else: # Если query.message нет (маловероятно для callback_query, но на всякий случай)
                await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2("⚠️ Неизвестное действие по callback."), parse_mode=ParseMode.MARKDOWN_V2)
        except Exception: 
            # Если редактирование не удалось, отправляем новое сообщение
            await context.bot.send_message(chat_id=admin_user.id, text=escape_markdown_v2("⚠️ Неизвестное действие по callback (ошибка редактирования)."), parse_mode=ParseMode.MARKDOWN_V2)


 
async def set_bot_commands(application: Application):
    """Устанавливает списки команд для кнопки меню бота в зависимости от типа пользователя."""
    
    common_commands = [
        BotCommand("start", "🚀 Начало работы / Статус / Подать заявку"),
        BotCommand("help", "ℹ️ Помощь по командам"),
    ]
    
    # Команды, доступные после авторизации (но не админские)
    authorized_user_commands = [
        BotCommand("checkin", "➡️ Отметить приход"),
        BotCommand("checkout", "⬅️ Отметить уход"),
        BotCommand("request_manual_checkin", "🛠️ Запросить ручную отметку"), # <--- ВОТ ЗДЕСЬ ДОБАВЛЯЕМ
    ]
    
    # Команды только для админов (добавляются к common и authorized)
    admin_specific_commands = [
        BotCommand("admin_authorize", "👑 Авторизовать пользователя"),
        BotCommand("on_shift", "Посмотреть, кто на смене"),
        BotCommand("admin_pending_users", "⏳ Заявки на доступ"),
        BotCommand("admin_export_attendance", "📊 Экспорт отчета о посещаемости"),
        BotCommand("admin_manual_checkins", "🛠️ Ручные заявки на приход"),
        BotCommand("restart", "🔄 Перезапустить бота"),
    ]

    # Эта строка объединяет common_commands и (теперь обновленные) authorized_user_commands.
    # Таким образом, default_commands_for_all будет содержать и /request_manual_checkin
    default_commands_for_all = common_commands + authorized_user_commands 
    try:
        # Эти команды будут видны всем пользователям, которые не являются администраторами.
        # (И неавторизованным, и авторизованным не-админам).
        await application.bot.set_my_commands(default_commands_for_all)
        logger.info(f"Команды по умолчанию ({len(default_commands_for_all)} шт.) установлены для всех пользователей (не админов).")
    except Exception as e:
        logger.error(f"Ошибка при установке команд по умолчанию для всех: {e}", exc_info=True)

    # Эта строка также использует обновленные authorized_user_commands
    admin_full_commands = common_commands + authorized_user_commands + admin_specific_commands
    for admin_id in ADMIN_TELEGRAM_IDS:
        try:
            await application.bot.set_my_commands(admin_full_commands, scope=BotCommandScopeChat(chat_id=admin_id))
            logger.info(f"Расширенные команды ({len(admin_full_commands)} шт.) установлены для администратора {admin_id}.")
        except Exception as e:
            logger.error(f"Ошибка при установке команд для администратора {admin_id}: {e}", exc_info=True)



def main() -> None:
    db.init_db() 

    # Создание экземпляра Application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # === 1. ConversationHandler для ПОДАЧИ ЗАЯВКИ НА ДОСТУП ===
    application_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_callback_handler, pattern='^apply_for_access$')],
        states={
            ASK_FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_full_name)],
            ASK_DEPARTMENT: [
                CallbackQueryHandler(process_department_selection, pattern='^reg_select_dept_'),
                CallbackQueryHandler(process_department_selection, pattern='^reg_cancel_direct$')
            ],
        },
        fallbacks=[CommandHandler('cancel_application', cancel_application_dialog)],
        name="application_flow", 
        per_user=True, 
        per_chat=True
    )

    # === 2. ConversationHandler для ЭКСПОРТА ОТЧЕТА ===
    export_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("admin_export_attendance", start_export_dialog)],
        states={
            SELECT_SECTOR: [
                CallbackQueryHandler(select_sector_callback, pattern="^export_sector_(СС|ВИ|ОП|ALL)$"), 
                CallbackQueryHandler(cancel_export_dialog, pattern="^export_cancel_dialog$") 
            ],
            SELECT_PERIOD: [
                CallbackQueryHandler(ask_export_start_date, pattern="^export_period_custom$"),
                CallbackQueryHandler(select_period_callback, pattern="^export_period_(today|yesterday|this_week|last_week|this_month|last_month)$"), 
                CallbackQueryHandler(start_export_dialog, pattern="^export_back_to_sector_selection$"),
                CallbackQueryHandler(cancel_export_dialog, pattern="^export_cancel_dialog$")
            ],
            GET_EXPORT_START_DATE: [CallbackQueryHandler(process_export_start_date)],
            GET_EXPORT_END_DATE: [CallbackQueryHandler(process_export_end_date)],
            CONFIRM_EXPORT: [
                CallbackQueryHandler(confirm_export_callback, pattern="^export_confirm_yes$"),
                CallbackQueryHandler(confirm_export_callback, pattern="^export_back_to_period_selection$"), 
                CallbackQueryHandler(cancel_export_dialog, pattern="^export_cancel_dialog$")
            ],
        },
        fallbacks=[
            CommandHandler("cancel_export", cancel_export_dialog),
            CallbackQueryHandler(cancel_export_dialog, pattern="^export_cancel_dialog$")
        ],
        name="export_flow",
        per_user=True,
        per_chat=True
    )

    # === 3. ConversationHandler для ЗАПРОСА РУЧНОЙ ОТМЕТКИ ПОЛЬЗОВАТЕЛЕМ ===
    manual_checkin_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("request_manual_checkin", request_manual_checkin_start)],
        states={
            REQUEST_MANUAL_CHECKIN_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_manual_checkin_time)
            ],
        },
        fallbacks=[CommandHandler("cancel_manual_checkin", cancel_manual_checkin_dialog)],
        name="manual_checkin_request_flow",
        per_user=True,
        per_chat=True
    )

    # === 4. ConversationHandler для ОБРАБОТКИ РУЧНЫХ ЗАЯВОК АДМИНИСТРАТОРОМ ===
    admin_manual_checkins_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("admin_manual_checkins", admin_manual_checkins_start)],
        states={
            ADMIN_LIST_MANUAL_REQUESTS: [
                CallbackQueryHandler(admin_select_manual_request, pattern="^admin_process_req_"),
                        
                CallbackQueryHandler(approve_all_requests_callback, pattern="^admin_approve_all$"),

                CallbackQueryHandler(admin_cancel_manual_checkins_dialog, pattern="^admin_cancel_manual_dialog$") 
            ],
            ADMIN_PROCESS_SINGLE_REQUEST: [
                CallbackQueryHandler(admin_process_request_action, pattern="^admin_req_") 
            ],
            ADMIN_ENTER_NEW_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_receive_new_time)
            ],
            ADMIN_CONFIRM_REQUEST_DECISION: [
                CallbackQueryHandler(admin_handle_final_confirmation, pattern="^admin_confirm_final_")
            ],
        },
        fallbacks=[
            CommandHandler("admin_cancel_manual_checkins", admin_cancel_manual_checkins_dialog),
            # Кстати, сюда было бы логично перенести и CallbackQueryHandler для отмены,
            # чтобы он работал из любого состояния диалога, а не только из первого.
            # CallbackQueryHandler(admin_cancel_manual_checkins_dialog, pattern="^admin_cancel_manual_dialog$")
        ],
        name="admin_manual_checkins_flow",
        per_user=True, 
        per_chat=True, 
    )
    
    # === Регистрация ВСЕХ обработчиков ===
    # Сначала добавляем ConversationHandlers
    application.add_handler(admin_manual_checkins_conv_handler) 
    application.add_handler(application_conv_handler)
    application.add_handler(export_conv_handler)
    application.add_handler(manual_checkin_conv_handler) 

    # Затем остальные обработчики
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("checkin", checkin_command))
    application.add_handler(CommandHandler("checkout", checkout_command))
    application.add_handler(CommandHandler("restart", restart_command)) 
    application.add_handler(CommandHandler("on_shift", on_shift_command, filters=AdminFilter()))
    application.add_handler(CommandHandler("admin_authorize", admin_authorize_command))
    application.add_handler(CommandHandler("admin_pending_users", admin_pending_users_command))
    application.add_handler(CallbackQueryHandler(on_shift_button_press, pattern=r"^on_shift_"))
    
    application.add_handler(MessageHandler(filters.LOCATION, location_handler))
    
    # Этот обработчик для других админских действий (просмотр заявки на доступ и т.д.)
    application.add_handler(CallbackQueryHandler(admin_action_callback_handler, pattern=r'^(view_user_app:|card_auth_app:|card_reject_app:|focus_in_list:|paginate_list:)'))
    
    # === Установка команд бота ===
    async def post_init_hook(app: Application):
        await set_bot_commands(app) # Убедитесь, что функция set_bot_commands определена и работает корректно
    application.post_init = post_init_hook

    # === Запуск бота ===
    logger.info("Бот запущен и готов к работе...")
    application.run_polling()

if __name__ == "__main__":
    main()











