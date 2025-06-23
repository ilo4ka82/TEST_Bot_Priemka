import sqlite3
import logging
from datetime import datetime
import pytz
from pytz import utc
from config import DATABASE_PATH


logger = logging.getLogger(__name__)


MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# В файле database_operations.py
def init_db():
    conn = None 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # --- Таблица users ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                application_full_name TEXT,
                application_department TEXT,
                is_authorized BOOLEAN DEFAULT FALSE,
                is_admin BOOLEAN DEFAULT FALSE,
                registration_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                application_status TEXT DEFAULT 'none' 
            )
        ''')
        logger.info("Таблица 'users' проверена/создана.")

        # --- Таблица work_sessions ---
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                check_in_time DATETIME NOT NULL, 
                check_out_time DATETIME,          
                duration_minutes INTEGER,
                latitude REAL,
                longitude REAL,
                checkin_type TEXT NOT NULL DEFAULT 'geo',
                sector_id TEXT,  -- <--- ВОТ ЭТА СТРОКА ДОБАВЛЕНА
                FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
            )
        ''')
        logger.info("Таблица 'work_sessions' проверена/создана.")

        # Проверка и добавление столбца checkin_type в work_sessions, если его нет (для существующих БД)
        # ЭТОТ БЛОК МОЖНО ОСТАВИТЬ, ОН НЕ МЕШАЕТ
        cursor.execute("PRAGMA table_info(work_sessions)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'checkin_type' not in columns: # Он уже должен быть из CREATE TABLE выше, но проверка не повредит
            try: # Добавим try-except на случай, если столбец уже есть из-за предыдущих попыток
                cursor.execute("ALTER TABLE work_sessions ADD COLUMN checkin_type TEXT NOT NULL DEFAULT 'geo'")
                logger.info("Столбец 'checkin_type' добавлен в таблицу 'work_sessions' через ALTER.")
            except sqlite3.OperationalError as alter_e:
                if "duplicate column name" in str(alter_e).lower():
                    logger.info("Столбец 'checkin_type' уже существует в 'work_sessions'.")
                else:
                    raise # Перевыбрасываем, если это другая ошибка ALTER
        
        # Проверка и добавление столбца sector_id в work_sessions, если его нет (для существующих БД)
        # Это полезно, если вы не хотите удалять БД при каждом изменении схемы
        cursor.execute("PRAGMA table_info(work_sessions)") # Получаем свежую информацию о столбцах
        columns = [column[1] for column in cursor.fetchall()]
        if 'sector_id' not in columns:
            try:
                cursor.execute("ALTER TABLE work_sessions ADD COLUMN sector_id TEXT")
                logger.info("Столбец 'sector_id' добавлен в таблицу 'work_sessions' через ALTER.")
            except sqlite3.OperationalError as alter_e_sector:
                if "duplicate column name" in str(alter_e_sector).lower():
                    logger.info("Столбец 'sector_id' уже существует в 'work_sessions'.")
                else:
                    raise 
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS manual_checkin_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                requested_checkin_time TEXT NOT NULL, 
                request_timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, 
                status TEXT NOT NULL DEFAULT 'pending', 
                admin_id_processed INTEGER,
                processed_timestamp TEXT,
                final_checkin_time TEXT, 
                FOREIGN KEY (user_id) REFERENCES users(telegram_id),
                FOREIGN KEY (admin_id_processed) REFERENCES users(telegram_id)
            )
        ''')
        logger.info("Таблица 'manual_checkin_requests' проверена/создана.")
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def add_or_update_user(telegram_id: int, username: str = None, first_name: str = None, last_name: str = None):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO users (telegram_id, username, first_name, last_name, registration_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username = COALESCE(excluded.username, users.username),
                first_name = COALESCE(excluded.first_name, users.first_name),
                last_name = COALESCE(excluded.last_name, users.last_name)
        ''', (telegram_id, username, first_name, last_name, datetime.now(tz=utc)))
        conn.commit()
        logger.info(f"Пользователь {telegram_id} ({first_name}) добавлен/обновлен в БД.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Ошибка при добавлении/обновлении пользователя {telegram_id}: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_user(telegram_id: int):
    """
    Получает пользователя по ID и возвращает его данные в виде СЛОВАРЯ.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        
        # --- ВОТ ОНО, ГЛАВНОЕ ИСПРАВЛЕНИЕ ---
        # Если пользователь найден (user_row не None),
        # конвертируем специальный объект sqlite3.Row в обычный словарь.
        if user_row:
            return dict(user_row)
        return None # Если пользователь не найден, возвращаем None
        # ------------------------------------


    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении пользователя {telegram_id}: {e}")
        return None
    finally:
        if conn:
            conn.close()

def is_user_authorized(telegram_id: int) -> bool:
    """
    Проверяет, авторизован ли пользователь.
    :param telegram_id: Telegram ID пользователя.
    :return: True, если пользователь существует и авторизован, иначе False.
    """
    user_data = get_user(telegram_id) 
    if user_data and user_data['is_authorized']: # is_authorized BOOLEAN (0 или 1)
        return True
    return False

def authorize_user(telegram_id_to_authorize: int, authorizing_admin_id: int):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        user_exists = get_user(telegram_id_to_authorize)
        if not user_exists:
            logger.warning(f"Попытка авторизации несуществующего пользователя {telegram_id_to_authorize} администратором {authorizing_admin_id}.")
            return False, f"Пользователь с ID {telegram_id_to_authorize} не найден в базе."
        cursor.execute('''
            UPDATE users SET is_authorized = TRUE, application_status = 'approved'
            WHERE telegram_id = ?
        ''', (telegram_id_to_authorize,))
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Пользователь {telegram_id_to_authorize} авторизован администратором {authorizing_admin_id} (статус 'approved').")
            return True, f"Пользователь ID {telegram_id_to_authorize} успешно авторизован."
        else:
            if user_exists['is_authorized'] and user_exists['application_status'] == 'approved':
                 return True, f"Пользователь ID {telegram_id_to_authorize} уже был авторизован ранее."
            logger.warning(f"Не удалось авторизовать пользователя {telegram_id_to_authorize} (rowcount=0, не был уже авторизован или статус не 'approved').")
            return False, f"Не удалось авторизовать пользователя ID {telegram_id_to_authorize}."
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при авторизации пользователя {telegram_id_to_authorize}: {e}")
        return False, f"Произошла ошибка базы данных при авторизации пользователя {telegram_id_to_authorize}."
    finally:
        if conn:
            conn.close()

def list_pending_users():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT telegram_id, username, first_name, last_name, registration_date, application_full_name, application_department 
            FROM users 
            WHERE application_status = 'pending' AND is_authorized = FALSE
            ORDER BY registration_date ASC 
        """)
        users = cursor.fetchall()
        return users
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении списка ожидающих пользователей: {e}")
        return []
    finally:
        if conn:
            conn.close()

def submit_application(telegram_id: int, full_name: str, department: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        user = get_user(telegram_id)
        if not user:
            logger.error(f"Попытка подать заявку от незарегистрированного пользователя {telegram_id}")
            return False, "Произошла ошибка. Пожалуйста, попробуйте сначала /start."
        if user['application_status'] == 'pending':
            return False, "Вы уже подали заявку. Ожидайте подтверждения администратором."
        if user['application_status'] == 'approved' or user['is_authorized']:
            return False, "Ваша заявка уже одобрена, и вы авторизованы."
        cursor.execute('''
            UPDATE users 
            SET application_status = 'pending', 
                application_full_name = ?, 
                application_department = ?
            WHERE telegram_id = ?
        ''', (full_name, department, telegram_id))
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Пользователь {telegram_id} подал заявку на доступ (статус 'pending') с ФИО: {full_name}, Отдел: {department}.")
            return True, "✅ Ваша заявка на доступ успешно подана! Администратор рассмотрит ее в ближайшее время."
        else:
            logger.warning(f"Не удалось обновить статус заявки для {telegram_id} (rowcount=0).")
            return False, "Не удалось подать заявку. Попробуйте еще раз."
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при подаче заявки пользователем {telegram_id}: {e}")
        return False, "Произошла ошибка базы данных при подаче заявки."
    finally:
        if conn:
            conn.close()

def record_check_in(user_id: int, latitude: float, longitude: float) -> tuple[bool, str]:
    """
    Записывает время прихода по геолокации.
    1. Проверяет, что нет других активных сессий.
    2. ТЕПЕРЬ СОХРАНЯЕТ ВРЕМЯ В БАЗУ В ФОРМАТЕ UTC.
    3. Находит и записывает департамент пользователя (sector_id).
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()


        # Шаг 1: Проверка на активную сессию (без изменений)
        cursor.execute("SELECT session_id FROM work_sessions WHERE telegram_id = ? AND check_out_time IS NULL", (user_id,))
        existing_session = cursor.fetchone()
        if existing_session:
            logger.warning(f"DB: Пользователь {user_id} попытался отметиться на приходе, уже имея активную сессию.")
            return (False, "❌ Вы уже отметились на приходе. Сначала нужно отметить уход.")


        # Шаг 2: Получение департамента (без изменений)
        cursor.execute("SELECT application_department FROM users WHERE telegram_id = ?", (user_id,))
        user_record = cursor.fetchone()
        user_department = user_record['application_department'] if user_record else None


        # --- НАЧАЛО ГЛАВНОГО ИСПРАВЛЕНИЯ: ПЕРЕХОДИМ НА UTC ---
        # Шаг 3: Получаем текущее МОСКОВСКОЕ время (для сообщения пользователю)
        moscow_time_now = datetime.now(MOSCOW_TZ)
        
        # Шаг 4: Конвертируем его в UTC (для записи в базу)
        utc_time_now = moscow_time_now.astimezone(pytz.utc)
        checkin_time_utc_str_for_db = utc_time_now.strftime('%Y-%m-%d %H:%M:%S')
        # --- КОНЕЦ ГЛАВНОГО ИСПРАВЛЕНИЯ ---


        # Шаг 5: Записываем все данные, ИСПОЛЬЗУЯ СТРОКУ UTC
        cursor.execute("""
            INSERT INTO work_sessions (telegram_id, check_in_time, checkin_type, latitude, longitude, sector_id)
            VALUES (?, ?, 'geo', ?, ?, ?)
        """, (user_id, checkin_time_utc_str_for_db, latitude, longitude, user_department))
        
        conn.commit()
        
        # В сообщении пользователю показываем МОСКОВСКОЕ время, которое он и ожидает увидеть
        time_str_for_message = moscow_time_now.strftime('%H:%M:%S')
        logger.info(f"DB: Пользователь {user_id} успешно отметил приход по гео в {time_str_for_message} (МСК). Запись в БД: {checkin_time_utc_str_for_db} (UTC). Департамент: '{user_department}'.")
        return (True, f"✅ Вы успешно отметили приход в {time_str_for_message}!")


    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"DB_ERROR: Ошибка при записи прихода для {user_id}: {e}", exc_info=True)
        return (False, "❌ Произошла ошибка базы данных при попытке отметить приход.")
    finally:
        if conn:
            conn.close()

def record_check_out(user_id: int) -> tuple[bool, str]:
    """
    Записывает время ухода, ВЫЧИСЛЯЕТ ДЛИТЕЛЬНОСТЬ СЕССИИ,
    и корректно работает с UTC временем из базы.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        # ШАГ 1: Находим ID сессии И ВРЕМЯ ПРИХОДА (которое в UTC)
        cursor.execute("""
            SELECT session_id, check_in_time FROM work_sessions
            WHERE telegram_id = ? AND check_out_time IS NULL
            ORDER BY check_in_time DESC
            LIMIT 1
        """, (user_id,))
        
        last_session = cursor.fetchone()


        if not last_session:
            logger.warning(f"DB: Пользователь {user_id} попытался уйти, не имея активных сессий.")
            return (False, "❌ Вы не были отмечены на приходе. Невозможно отметить уход.")


        session_to_close_id = last_session['session_id']
        check_in_time_utc_str = last_session['check_in_time']
        
        # ШАГ 2: Превращаем время прихода из строки UTC в объект времени МОСКВЫ
        check_in_utc_dt = datetime.strptime(check_in_time_utc_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
        check_in_moscow_dt = check_in_utc_dt.astimezone(MOSCOW_TZ)
        
        # ШАГ 3: Получаем текущее время ухода в МОСКВЕ
        checkout_moscow_dt = datetime.now(MOSCOW_TZ)
        
        # ШАГ 4: ВЫЧИСЛЯЕМ ДЛИТЕЛЬНОСТЬ СЕССИИ
        duration = checkout_moscow_dt - check_in_moscow_dt
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        
        # ШАГ 5: Обновляем сессию в базе
        checkout_time_str_for_db = checkout_moscow_dt.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            "UPDATE work_sessions SET check_out_time = ? WHERE session_id = ?",
            (checkout_time_str_for_db, session_to_close_id)
        )
        conn.commit()
        
        # ШАГ 6: Формируем новое, информативное сообщение
        checkout_time_display = checkout_moscow_dt.strftime('%H:%M:%S')
        message = (
            f"✅ Вы успешно отметили уход в {checkout_time_display}.\n\n"
            f"⏱️ **Продолжительность сессии:** {duration_str}\n\n"
            f"Хорошего вечера!"
        )
        
        logger.info(f"DB: Пользователь {user_id} успешно отметил уход для сессии {session_to_close_id}. Длительность: {duration_str}.")
        return (True, message)


    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"DB_ERROR: Ошибка при записи ухода для {user_id}: {e}", exc_info=True)
        return (False, "❌ Произошла ошибка базы данных при попытке отметить уход.")
    finally:
        if conn:
            conn.close()

        
def reject_application(telegram_id_to_reject: int, rejecting_admin_id: int, reason: str = None): # reason пока не используется, но оставим
    conn = None 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        user_to_reject = get_user(telegram_id_to_reject) 

        if not user_to_reject:
            logger.warning(f"Попытка отклонить заявку несуществующего пользователя {telegram_id_to_reject} администратором {rejecting_admin_id}.")
            return False, f"Пользователь с ID {telegram_id_to_reject} не найден в базе."

        if user_to_reject['is_authorized']: 
            logger.info(f"Попытка отклонить заявку уже авторизованного пользователя {telegram_id_to_reject}.")
            return False, f"Пользователь ID {telegram_id_to_reject} уже авторизован. Отклонение заявки невозможно."
        
        if user_to_reject['application_status'] == 'rejected':
            logger.info(f"Заявка пользователя {telegram_id_to_reject} уже была отклонена ранее.")
            return True, f"Заявка пользователя ID {telegram_id_to_reject} уже была отклонена ранее."

        if user_to_reject['application_status'] != 'pending':
            logger.warning(f"Попытка отклонить заявку пользователя {telegram_id_to_reject} со статусом '{user_to_reject['application_status']}' (ожидался 'pending').")
            return False, f"Заявка пользователя ID {telegram_id_to_reject} не находится в статусе 'pending' (текущий статус: {user_to_reject['application_status']})."

        cursor.execute("""
            UPDATE users 
            SET application_status = 'rejected', 
                is_authorized = FALSE,
                application_full_name = NULL,
                application_department = NULL
            WHERE telegram_id = ? AND application_status = 'pending'
        """, (telegram_id_to_reject,))
        conn.commit()

        if cursor.rowcount > 0:
            logger.info(f"Заявка пользователя {telegram_id_to_reject} отклонена администратором {rejecting_admin_id}. Данные заявки очищены.")
            return True, f"Заявка пользователя ID {telegram_id_to_reject} успешно отклонена."
        else:
            logger.warning(f"Не удалось отклонить заявку пользователя {telegram_id_to_reject} (rowcount=0 при UPDATE). Проверка текущего состояния...")
            current_status_check = get_user(telegram_id_to_reject) 
            if current_status_check:
                if current_status_check['application_status'] == 'rejected':
                    return True, f"Заявка пользователя ID {telegram_id_to_reject} уже была отклонена."
                elif current_status_check['is_authorized']:
                     return False, f"Пользователь ID {telegram_id_to_reject} уже авторизован."
                else:
                    return False, f"Не удалось обновить статус заявки для пользователя ID {telegram_id_to_reject} (текущий статус: {current_status_check['application_status']})."
            else: 
                return False, f"Не удалось найти пользователя ID {telegram_id_to_reject} после попытки отклонения."

    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при отклонении заявки пользователя {telegram_id_to_reject}: {e}", exc_info=True)
        return False, f"Произошла ошибка базы данных при отклонении заявки пользователя {telegram_id_to_reject}."
    finally:
        if conn:
            conn.close()

def get_attendance_data_for_period(start_date: datetime, end_date: datetime, sector_key: str = None) -> list:
    """
    Получает сырые данные о сессиях за период. Длительность будет рассчитана позже.
    """
    conn = get_db_connection()
    attendance_data = []
    try:
        cursor = conn.cursor()
        
        # УБРАЛИ 'duration_minutes' ИЗ ЗАПРОСА. ОН НАМ БОЛЬШЕ НЕ НУЖЕН.
        query = """
            SELECT 
                u.application_full_name,
                u.username,
                u.application_department,
                ws.check_in_time AS session_start_time, 
                ws.check_out_time AS session_end_time
            FROM work_sessions ws
            JOIN users u ON ws.telegram_id = u.telegram_id
            WHERE ws.check_in_time BETWEEN ? AND ? 
        """
        
        params = [start_date, end_date]
        
        if sector_key and sector_key.upper() != 'ALL':
            query += " AND UPPER(u.application_department) = ? "
            params.append(sector_key.upper())
        
        query += " ORDER BY u.application_full_name ASC, ws.check_in_time ASC"
        
        cursor.execute(query, tuple(params))
        
        columns = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            attendance_data.append(dict(zip(columns, row)))
        
        logger.info(f"DB: Получено {len(attendance_data)} записей о посещаемости для отчета.")
    
    except sqlite3.Error as e:
        logger.error(f"DB: Ошибка SQLite при получении данных о посещаемости: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
    return attendance_data

    
    return attendance_data


def get_unique_departments() -> list:
    """
    Извлекает список уникальных непустых названий департаментов (секторов) из таблицы users.
    """
    conn = get_db_connection()
    departments = []
    try:
        cursor = conn.cursor()
        # Выбираем только не-NULL и непустые значения, отсортированные для единообразия
        cursor.execute("""
            SELECT DISTINCT application_department 
            FROM users 
            WHERE application_department IS NOT NULL AND application_department != ''
            ORDER BY application_department ASC
        """)
        rows = cursor.fetchall()
        # Преобразуем результат (список кортежей) в список строк
        departments = [row['application_department'] for row in rows if row['application_department']] # Дополнительная проверка на всякий случай
        if departments:
            logger.info(f"DB: Получены уникальные секторы из таблицы users: {departments}")
        else:
            logger.info("DB: Уникальные секторы в таблице users не найдены или все пустые/NULL.")
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при получении уникальных секторов: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при получении уникальных секторов: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return departments

def add_manual_checkin_request(user_id: int, requested_checkin_time: datetime) -> bool:
    """
    Добавляет новую заявку на ручную отметку прихода в базу данных.
    Принимает datetime объект (в UTC) и сохраняет его как текст.
    """
    conn = None
    try:
        conn = get_db_connection() 
        cursor = conn.cursor()
        
        # Мы берем datetime объект (который УЖЕ в UTC из bot_main.py)
        # и просто форматируем его в строку для SQLite.
        requested_time_str = requested_checkin_time.strftime('%Y-%m-%d %H:%M:%S')
        # ------------------------------------
        
        cursor.execute("""
            INSERT INTO manual_checkin_requests 
            (user_id, requested_checkin_time, request_timestamp, status)
            VALUES (?, ?, datetime('now', 'localtime'), 'pending') 
        """, (user_id, requested_time_str))
        conn.commit()
        
        logger.info(f"DB: Новая заявка на ручную отметку прихода добавлена для user_id={user_id}, время={requested_time_str} (UTC)")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при добавлении заявки на ручную отметку для user_id={user_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def get_pending_manual_checkin_requests() -> list:
    """
    Возвращает список ожидающих ручных заявок в виде СЛОВАРЕЙ, 
    объединенный с данными пользователя.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row # Это оставляем, это полезно
        cursor = conn.cursor()
        
        sql_query = """
            SELECT
                req.request_id,
                req.user_id,
                req.requested_checkin_time,
                req.request_timestamp,
                u.first_name,
                u.last_name,
                u.username,
                u.application_department
            FROM manual_checkin_requests req
            JOIN users u ON req.user_id = u.telegram_id
            WHERE req.status = 'pending'
            ORDER BY req.request_timestamp ASC
        """
        
        cursor.execute(sql_query)
        requests_rows = cursor.fetchall() # Получаем список sqlite3.Row
        
        # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Превращаем каждый sqlite3.Row в обычный dict ---
        requests_dicts = [dict(row) for row in requests_rows]
        # --------------------------------------------------------------------------


        logger.info(f"DB: Найдено {len(requests_dicts)} ожидающих заявок на ручную отметку.")
        return requests_dicts # Возвращаем список словарей
        
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении списка ручных заявок: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def approve_all_pending_manual_checkins(admin_id: int) -> tuple[int, int]:
    """
    Одобряет все ожидающие ручные заявки в рамках одной транзакции.
    Возвращает кортеж (количество успешных, количество неуспешных).
    """
    conn = None
    approved_count = 0
    failed_count = 0
    
    try:
        # Получаем все заявки для обработки
        pending_requests = get_pending_manual_checkin_requests()
        if not pending_requests:
            return (0, 0)


        conn = get_db_connection()
        cursor = conn.cursor()
        
        processed_time_str = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')


        for req in pending_requests:
            try:
                # Шаг 1: Добавляем рабочую сессию
                cursor.execute(
                    "INSERT INTO work_sessions (telegram_id, check_in_time, checkin_type, sector_id) VALUES (?, ?, 'manual_admin', ?)",
                    (req['user_id'], req['requested_checkin_time'], req['application_department'])
                )
                
                # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ: МЕНЯЕМ ИМЯ СТОЛБЦА ---
                cursor.execute(
                    "UPDATE manual_checkin_requests SET status = 'approved', admin_id_processed = ?, processed_timestamp = ? WHERE request_id = ?",
                    (admin_id, processed_time_str, req['request_id'])
                )
                # -------------------------------------------------
                
                approved_count += 1
            except sqlite3.Error as e:
                logger.error(f"DB_ERROR: Ошибка при массовом одобрении заявки {req['request_id']}: {e}")
                failed_count += 1
        
        conn.commit()
        logger.info(f"DB: Массовое одобрение завершено админом {admin_id}. Успешно: {approved_count}, Ошибки: {failed_count}.")
        return (approved_count, failed_count)


    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Критическая ошибка в approve_all_pending_manual_checkins: {e}")
        if conn:
            conn.rollback() 
        return (0, len(pending_requests) if 'pending_requests' in locals() else 0)
    finally:
        if conn:
            conn.close()




def get_manual_checkin_request_by_id(request_id: int):
    """
    Возвращает детали конкретной заявки в виде СЛОВАРЯ.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row # Это оставляем
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                mcr.request_id, 
                mcr.user_id, 
                mcr.requested_checkin_time,
                mcr.status,
                u.username,
                u.first_name,
                u.last_name,
                u.application_department
            FROM manual_checkin_requests mcr
            JOIN users u ON mcr.user_id = u.telegram_id
            WHERE mcr.request_id = ?
        """, (request_id,))
        
        request_data_row = cursor.fetchone() # Получаем sqlite3.Row или None
        
        # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Превращаем sqlite3.Row в dict ---
        if request_data_row:
            logger.info(f"DB: Получена заявка на ручную отметку с ID={request_id}.")
            return dict(request_data_row) # Возвращаем СЛОВАРЬ
        else:
            logger.warning(f"DB: Заявка на ручную отметку с ID={request_id} не найдена.")
            return None # Если ничего не найдено, возвращаем None
        # -----------------------------------------------------------------


    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении ручной заявки по ID={request_id}: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()

def approve_manual_checkin_request(request_id: int, admin_id: int, final_checkin_time_local: datetime, user_id: int, user_sector_key: str) -> bool:
    """
    Одобряет заявку на ручную отметку.
    ТЕПЕРЬ СОХРАНЯЕТ ВРЕМЯ В БАЗУ В ФОРМАТЕ UTC.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # --- НАЧАЛО ГЛАВНОГО ИСПРАВЛЕНИЯ: ПЕРЕХОДИМ НА UTC ---
        # Входное время final_checkin_time_local - это МОСКОВСКОЕ время.
        # Конвертируем его в UTC для записи в базу.
        final_checkin_time_utc = final_checkin_time_local.astimezone(pytz.utc)
        checkin_time_utc_str_for_db = final_checkin_time_utc.strftime('%Y-%m-%d %H:%M:%S')
        
        # Время обработки заявки тоже лучше хранить в UTC для единообразия
        processed_time_utc_str = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        # --- КОНЕЦ ГЛАВНОГО ИСПРАВЛЕНИЯ ---


        # Обновляем саму заявку, используя время в UTC
        cursor.execute("""
            UPDATE manual_checkin_requests
            SET status = 'approved',
                admin_id_processed = ?, 
                processed_timestamp = ?,
                final_checkin_time = ?
            WHERE request_id = ? AND status = 'pending'
        """, (admin_id, processed_time_utc_str, checkin_time_utc_str_for_db, request_id))
        
        if cursor.rowcount == 0:
            logger.warning(f"DB: Не удалось обновить заявку {request_id} для одобрения (возможно, уже обработана).")
            conn.rollback()
            return False


        # Создаем рабочую сессию, используя время в UTC
        cursor.execute("""
            INSERT INTO work_sessions (telegram_id, check_in_time, checkin_type, sector_id)
            VALUES (?, ?, 'manual_admin', ?)
        """, (user_id, checkin_time_utc_str_for_db, user_sector_key))
        
        conn.commit()
        
        # В логах для ясности указываем оба времени
        local_time_str = final_checkin_time_local.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"DB: Заявка {request_id} одобрена. Создана сессия для user_id={user_id}, сектор='{user_sector_key}'. Время (MSK): {local_time_str}, Время в БД (UTC): {checkin_time_utc_str_for_db}.")
        return True


    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"DB_ERROR: Ошибка при одобрении ручной заявки {request_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


def reject_manual_checkin_request(request_id: int, admin_id: int) -> bool:
    """
    Отклоняет заявку на ручную отметку и обновляет ее статус.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Время обработки всегда в UTC
        processed_time_utc_str = datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
            UPDATE manual_checkin_requests
            SET status = 'rejected',
                admin_id_processed = ?,
                processed_timestamp = ?
            WHERE request_id = ? AND status = 'pending'
        """, (admin_id, processed_time_utc_str, request_id))
        
        if cursor.rowcount == 0:
            logger.warning(f"DB: Не удалось отклонить заявку {request_id} (возможно, уже обработана или не найдена).")
            return False # Транзакцию откатывать не нужно, т.к. это одно действие
            
        conn.commit()
        logger.info(f"DB: Заявка {request_id} отклонена админом {admin_id}.")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при отклонении ручной заявки {request_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


def get_unique_user_departments() -> list:
    """
    Возвращает отсортированный список уникальных департаментов из таблицы users.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # ИСПОЛЬЗУЕМ ПРАВИЛЬНОЕ ИМЯ КОЛОНКИ: application_department
        cursor.execute("SELECT DISTINCT application_department FROM users WHERE application_department IS NOT NULL AND application_department != '' ORDER BY application_department ASC")
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении списка уникальных департаментов: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()



def get_active_users_by_department(department: str) -> list:
    """
    Возвращает УНИКАЛЬНЫЙ список пользователей с их САМОЙ ПОСЛЕДНЕЙ активной сессией.
    Данные отсортированы по ДЕПАРТАМЕНТУ, а затем по ФИО.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()

        sql_query = """
            SELECT
                u.application_full_name,
                u.username,
                u.application_department,
                t.max_check_in_time AS check_in_time
            FROM (
                SELECT
                    telegram_id,
                    MAX(check_in_time) AS max_check_in_time
                FROM work_sessions
                WHERE check_out_time IS NULL
                GROUP BY telegram_id
            ) AS t
            JOIN users u ON t.telegram_id = u.telegram_id
            WHERE
                (? = 'ALL' OR u.application_department = ?)
            -- --- ГЛАВНОЕ ИЗМЕНЕНИЕ: СОРТИРУЕМ ПО ФИО ---
            ORDER BY
                u.application_department, u.application_full_name ASC;
        """
        
        cursor.execute(sql_query, (department, department))
        results = cursor.fetchall()
        logger.info(f"DB_INFO: Умный запрос вернул {len(results)} уникальных строк.")
        return results

    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении активных пользователей для департамента '{department}': {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()













