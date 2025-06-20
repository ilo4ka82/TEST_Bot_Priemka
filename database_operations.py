import sqlite3
import logging
from datetime import datetime, timedelta
from pytz import timezone as pytz_timezone, utc
from config import DATABASE_PATH

logger = logging.getLogger(__name__)

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
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user_row = cursor.fetchone()
        return user_row
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

def record_check_in(telegram_id: int, latitude: float, longitude: float):
    check_in_time_aware_utc = datetime.now(tz=utc)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT session_id FROM work_sessions
            WHERE telegram_id = ? AND check_out_time IS NULL
        ''', (telegram_id,))
        active_session = cursor.fetchone()
        if active_session:
            return False, "У вас уже есть активная рабочая сессия. Сначала завершите ее командой /checkout."
        cursor.execute('''
            INSERT INTO work_sessions (telegram_id, check_in_time, latitude, longitude)
            VALUES (?, ?, ?, ?)
        ''', (telegram_id, check_in_time_aware_utc.strftime('%Y-%m-%d %H:%M:%S'), latitude, longitude))
        conn.commit()
        logger.info(f"Check-in записан для пользователя {telegram_id} в {check_in_time_aware_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC с координатами ({latitude}, {longitude})")
        desired_timezone_str = "Europe/Moscow" 
        local_tz_obj = pytz_timezone(desired_timezone_str)
        localized_check_in_time = check_in_time_aware_utc.astimezone(local_tz_obj)
        return True, f"Ваш приход отмечен в {localized_check_in_time.strftime('%Y-%m-%d %H:%M:%S')} ({desired_timezone_str})."
    except sqlite3.Error as e:
        logger.error(f"Ошибка при записи check-in для {telegram_id}: {e}")
        return False, "Произошла ошибка при записи вашего прихода. Пожалуйста, попробуйте позже."
    finally:
        if conn:
            conn.close()

def record_check_out(telegram_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    check_out_time_aware_utc = datetime.now(tz=utc)
    try:
        cursor.execute('''
            SELECT session_id, check_in_time FROM work_sessions
            WHERE telegram_id = ? AND check_out_time IS NULL
            ORDER BY check_in_time DESC LIMIT 1 
        ''', (telegram_id,))
        active_session = cursor.fetchone()
        if not active_session:
            return False, "У вас нет активной рабочей сессии. Сначала отметьте приход командой /checkin."
        session_id = active_session['session_id']
        try:
            check_in_time_str_from_db = active_session['check_in_time']
            naive_check_in_time = datetime.strptime(check_in_time_str_from_db.split('.')[0], '%Y-%m-%d %H:%M:%S')
            aware_check_in_time_utc = utc.localize(naive_check_in_time)
        except ValueError as ve:
            logger.error(f"Ошибка парсинга check_in_time '{active_session['check_in_time']}' для сессии {session_id}: {ve}")
            return False, "Ошибка формата времени в активной сессии. Обратитесь к администратору."
        duration_timedelta = check_out_time_aware_utc - aware_check_in_time_utc
        duration_minutes = int(duration_timedelta.total_seconds() / 60)
        cursor.execute('''
            UPDATE work_sessions
            SET check_out_time = ?, duration_minutes = ?
            WHERE session_id = ?
        ''', (check_out_time_aware_utc.strftime('%Y-%m-%d %H:%M:%S'), duration_minutes, session_id))
        conn.commit()
        logger.info(f"Check-out записан для пользователя {telegram_id}, сессия {session_id}. Время ухода UTC: {check_out_time_aware_utc.strftime('%Y-%m-%d %H:%M:%S')}, Продолжительность: {duration_minutes} мин.")
        hours = duration_minutes // 60
        minutes = duration_minutes % 60
        duration_readable_str = ""
        if hours > 0:
            duration_readable_str += f"{hours} ч. "
        duration_readable_str += f"{minutes} мин."
        desired_timezone_str = "Europe/Moscow"
        local_tz_obj = pytz_timezone(desired_timezone_str)
        localized_check_out_time = check_out_time_aware_utc.astimezone(local_tz_obj)
        return True, f"✅ Ваш уход отмечен в {localized_check_out_time.strftime('%Y-%m-%d %H:%M:%S')} ({desired_timezone_str}).\nПродолжительность сессии: {duration_readable_str}"
    except sqlite3.Error as e:
        logger.error(f"Ошибка SQLite при записи check-out для пользователя {telegram_id}: {e}")
        return False, "Произошла ошибка базы данных при отметке вашего ухода. Пожалуйста, попробуйте позже."
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при записи check-out для пользователя {telegram_id}: {e}", exc_info=True)
        return False, "Произошла непредвиденная ошибка при отметке вашего ухода. Пожалуйста, попробуйте позже."
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
    Получает данные о посещаемости за указанный период с опциональной фильтрацией по сектору.
    
    Args:
        start_date (datetime): Начало периода.
        end_date (datetime): Конец периода.
        sector_key (str, optional): Ключ сектора для фильтрации (например, "СС", "ВИ", "ОП", или "ALL").
    
    Returns:
        list: Список словарей с данными о посещаемости.
              Ожидаемые ключи в каждом словаре: 
              'application_full_name', 'username', 'application_department',
              'session_start_time', 'session_end_time', 'duration_minutes'.
    """
    conn = get_db_connection()
    attendance_data = []
    try:
        cursor = conn.cursor()
        
        query = """
            SELECT 
                u.application_full_name,
                u.username,
                u.application_department,
                ws.check_in_time AS session_start_time, 
                ws.check_out_time AS session_end_time,  
                ws.duration_minutes 
            FROM work_sessions ws
            JOIN users u ON ws.telegram_id = u.telegram_id  -- ИСПРАВЛЕНО УСЛОВИЕ JOIN
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
        
        logger.info(f"DB: Получено {len(attendance_data)} записей о посещаемости за период с {start_date.strftime('%Y-%m-%d')} по {end_date.strftime('%Y-%m-%d')}.")
        if sector_key and sector_key.upper() != 'ALL':
            logger.info(f"DB: Применен фильтр по сектору: {sector_key.upper()}")
    
    except sqlite3.Error as e:
        logger.error(f"DB: Ошибка SQLite при получении данных о посещаемости: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"DB: Непредвиденная ошибка при получении данных о посещаемости: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
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
    :param user_id: Telegram ID пользователя.
    :param requested_checkin_time: Запрошенное пользователем время прихода (объект datetime).
    :return: True в случае успеха, False в случае ошибки.
    """
    conn = None
    try:
        conn = get_db_connection() 
        cursor = conn.cursor()
        
        # Форматируем datetime в строку ISO8601 для SQLite
        requested_time_str = requested_checkin_time.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute("""
            INSERT INTO manual_checkin_requests 
            (user_id, requested_checkin_time, request_timestamp, status)
            VALUES (?, ?, datetime('now', 'localtime'), 'pending') 
        """, (user_id, requested_time_str)) # Используем datetime('now', 'localtime') для request_timestamp
        conn.commit()
        logger.info(f"DB: Новая заявка на ручную отметку прихода добавлена для user_id={user_id}, время={requested_time_str}")
        return True
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при добавлении заявки на ручную отметку для user_id={user_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def get_pending_manual_checkin_requests() -> list:
    """
    Возвращает список ожидающих ручных заявок, ОБЪЕДИНЕННЫЙ с данными пользователя (имя, департамент).
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
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
        requests = cursor.fetchall()
        logger.info(f"DB: Найдено {len(requests)} ожидающих заявок на ручную отметку.")
        return requests
        
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении списка ручных заявок: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def approve_all_pending_manual_checkins() -> tuple[int, int]:
    """
    Одобряет все ожидающие ручные заявки в рамках одной транзакции.
    """
    conn = None
    approved_count = 0
    failed_count = 0
    total_requests = 0
    
    try:
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        pending_requests = get_pending_manual_checkin_requests() 
        
        if not pending_requests:
            return (0, 0)

        total_requests = len(pending_requests)

        for req in pending_requests:
            try:
                # Шаг 1: Добавляем рабочую сессию
                cursor.execute(
                    "INSERT INTO work_sessions (telegram_id, check_in_time) VALUES (?, ?)",
                    (req['user_id'], req['requested_checkin_time'])
                )
                
                # --- ГЛАВНОЕ ИСПРАВЛЕНИЕ: УБИРАЕМ processed_by ---
                # Шаг 2: Обновляем статус заявки
                cursor.execute(
                    "UPDATE manual_checkin_requests SET status = 'approved' WHERE request_id = ?",
                    (req['request_id'],)
                )
                
                approved_count += 1
            except sqlite3.Error as e:
                logger.error(f"DB_ERROR: Ошибка при массовом одобрении заявки {req['request_id']}: {e}", exc_info=True)
                failed_count += 1
        
        conn.commit()
        logger.info(f"DB: Массовое одобрение завершено. Успешно: {approved_count}, Ошибки: {failed_count}.")
        return (approved_count, failed_count)

    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Критическая ошибка в approve_all_pending_manual_checkins: {e}", exc_info=True)
        if conn:
            conn.rollback() 
        return (0, total_requests)
    finally:
        if conn:
            conn.close()




def get_manual_checkin_request_by_id(request_id: int):
    """
    Возвращает детали конкретной заявки на ручную отметку по ее ID,
    включая информацию о пользователе.
    """
    conn = None
    request_data_row = None
    try:
        conn = get_db_connection()
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
                u.application_department -- Ключ сектора пользователя
            FROM manual_checkin_requests mcr
            JOIN users u ON mcr.user_id = u.telegram_id
            WHERE mcr.request_id = ?
        """, (request_id,))
        request_data_row = cursor.fetchone() # fetchone() возвращает один sqlite3.Row объект или None
        if request_data_row:
            logger.info(f"DB: Получена заявка на ручную отметку с ID={request_id}.")
        else:
            logger.warning(f"DB: Заявка на ручную отметку с ID={request_id} не найдена.")
    except sqlite3.Error as e:
        logger.error(f"DB_ERROR: Ошибка при получении ручной заявки по ID={request_id}: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    return request_data_row

def approve_manual_checkin_request(request_id: int, admin_id: int, final_checkin_time_local: datetime, 
                                   user_id: int, user_sector_key: str) -> bool:
    """
    Одобряет заявку на ручную отметку, обновляет ее статус и создает запись в work_sessions.
    final_checkin_time_local - это "наивный" datetime объект, представляющий локальное время
    (например, МСК), указанное администратором.
    Это время будет конвертировано в UTC для записи в work_sessions и manual_checkin_requests.final_checkin_time.
    user_sector_key - строковый ключ сектора (например, "СС"), который будет записан в work_sessions.sector_id.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION") # Начинаем транзакцию

        # 1. Конвертируем локальное время администратора (предположительно МСК) в UTC
        # Убедитесь, что 'Europe/Moscow' - правильный часовой пояс для ввода админом.
        try:
            admin_input_tz = pytz_timezone('Europe/Moscow')
            # Делаем "наивный" datetime "осведомленным" о своем часовом поясе
            final_checkin_time_local_aware = admin_input_tz.localize(final_checkin_time_local, is_dst=None)
            # Конвертируем в UTC
            final_checkin_time_utc = final_checkin_time_local_aware.astimezone(utc)
        except (pytz_timezone.UnknownTimeZoneError, ValueError, AttributeError) as tz_error:
            # ValueError может возникнуть, если localize не может обработать время (например, несуществующее из-за DST)
            logger.error(f"Ошибка конвертации времени для заявки {request_id}: {tz_error}", exc_info=True)
            conn.rollback()
            return False
            
        start_time_utc_str_for_session = final_checkin_time_utc.strftime('%Y-%m-%d %H:%M:%S')
        final_checkin_time_utc_str_for_request = final_checkin_time_utc.strftime('%Y-%m-%d %H:%M:%S')
        
        processed_time_utc_str = datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S') # Время обработки всегда в UTC

        # 2. Обновляем заявку в manual_checkin_requests
        cursor.execute("""
            UPDATE manual_checkin_requests
            SET status = 'approved',
                admin_id_processed = ?,
                processed_timestamp = ?,
                final_checkin_time = ? 
            WHERE request_id = ? AND status = 'pending' 
        """, (admin_id, processed_time_utc_str, final_checkin_time_utc_str_for_request, request_id))
        
        if cursor.rowcount == 0:
            logger.warning(f"DB: Не удалось обновить заявку {request_id} для одобрения (возможно, уже обработана или не найдена).")
            conn.rollback()
            return False

        # 3. Создаем запись в work_sessions
        # Предполагаем, что work_sessions.sector_id может хранить строковый ключ сектора.
        # Если work_sessions.sector_id должен быть числовым ID, эту логику нужно изменить.
        cursor.execute("""
            INSERT INTO work_sessions (telegram_id, check_in_time, checkin_type, sector_id)
            VALUES (?, ?, 'manual_admin', ?)
        """, (user_id, start_time_utc_str_for_session, user_sector_key))
        
        conn.commit() # Завершаем транзакцию успешно
        logger.info(f"DB: Заявка {request_id} одобрена админом {admin_id}. Создана сессия для user_id={user_id}, сектор='{user_sector_key}', время_utc='{start_time_utc_str_for_session}'.")
        return True

    except sqlite3.Error as e:
        if conn:
            conn.rollback() # Откатываем транзакцию при ошибке SQLite
        logger.error(f"DB_ERROR: SQLite ошибка при одобрении ручной заявки {request_id}: {e}", exc_info=True)
        return False
    except Exception as e_gen: # Ловим другие возможные ошибки (например, от pytz если не обработаны выше)
        if conn:
            conn.rollback()
        logger.error(f"DB_ERROR: Общая ошибка при одобрении ручной заявки {request_id}: {e_gen}", exc_info=True)
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













